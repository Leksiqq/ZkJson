using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace Net.Leksi.ZkJson;

internal class ZkJsonConverter : JsonConverter<ZkStub>
{
    private static Regex manySlashes = new("/{2,}");
    internal static int DOUBLE = 0x10000;
    private readonly MemoryStream _memoryStream = new();
    private readonly BinaryWriter _binWriter;
    private readonly BinaryReader _binReader;

    public ZkJsonConverter()
    {
        _binWriter = new BinaryWriter(_memoryStream);
        _binReader = new BinaryReader(_memoryStream);
    }
    public override ZkStub? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if((ZkJsonSerializer?)options.Converters.Where(c => c is ZkJsonSerializer).FirstOrDefault() is ZkJsonSerializer factory)
        {
            bool isRoot = false;
            if (factory.IsReady)
            {
                isRoot = true;
                string root = factory.Root.StartsWith("/") ? factory.Root[1..] : factory.Root;
                if (!string.IsNullOrEmpty(root))
                {
                    factory.PushPathComponent(root);
                }
                Delete(factory, factory.Path).Wait();
                if (factory.Deletion)
                {
                    while (reader.Read()) { }
                    factory.RunOps().Wait();
                    return null;
                }
            }
            if (reader.TokenType is JsonTokenType.StartObject)
            {
                factory.AddOp(Op.create(factory.Path, ToBytes((long)JsonValueKind.Object), factory.AclList, CreateMode.PERSISTENT));
                while (reader.Read())
                {
                    if (reader.TokenType is JsonTokenType.EndObject)
                    {
                        break;
                    }
                    if (reader.TokenType is not JsonTokenType.PropertyName)
                    {
                        throw new JsonException("Property name missed!");
                    }
                    string propertyName = reader.GetString() ?? throw new JsonException("Property name missed!");
                    if (!reader.Read())
                    {
                        throw new JsonException("Property value missed!");
                    }
                    factory.PushPathComponent(propertyName);
                    Read(ref reader, typeToConvert, options);
                    factory.PopPathComponent();
                }
            }
            else if(reader.TokenType is JsonTokenType.StartArray)
            {
                factory.AddOp(Op.create(factory.Path, ToBytes((long)JsonValueKind.Array), factory.AclList, CreateMode.PERSISTENT));
                long pos = 0;
                while (reader.Read())
                {
                    if (reader.TokenType is JsonTokenType.EndArray)
                    {
                        break;
                    }
                    factory.PushPathComponent(pos.ToString("D18"));
                    Read(ref reader, typeToConvert, options);
                    factory.PopPathComponent();
                    ++pos;
                }
            }
            else
            {
                byte[]? bytes = null;
                switch (reader.TokenType)
                {
                    case JsonTokenType.String:
                        factory.AddOp(Op.create(factory.Path, ToBytes((long)JsonValueKind.String), factory.AclList, CreateMode.PERSISTENT));
                        bytes = ToBytes(reader.GetString()!);
                        break;
                    case JsonTokenType.Number:
                        if (reader.TryGetInt64(out long longValue))
                        {
                            factory.AddOp(Op.create(factory.Path, ToBytes((long)JsonValueKind.Number), factory.AclList, CreateMode.PERSISTENT));
                            bytes = ToBytes(longValue);
                        }
                        else if (reader.TryGetDouble(out double doubleValue))
                        {
                            factory.AddOp(Op.create(factory.Path, ToBytes((int)JsonValueKind.Number | DOUBLE), factory.AclList, CreateMode.PERSISTENT));
                            bytes = ToBytes(doubleValue);
                        }
                        break;
                    case JsonTokenType.True:
                        factory.AddOp(Op.create(factory.Path, ToBytes((int)JsonValueKind.True), factory.AclList, CreateMode.PERSISTENT));
                        break;
                    case JsonTokenType.False:
                        factory.AddOp(Op.create(factory.Path, ToBytes((int)JsonValueKind.False), factory.AclList, CreateMode.PERSISTENT));
                        break;
                    case JsonTokenType.Null:
                        factory.AddOp(Op.create(factory.Path, ToBytes((int)JsonValueKind.Null), factory.AclList, CreateMode.PERSISTENT));
                        break;
                }
                if (bytes is { })
                {
                    factory.AddOp(
                        Op.create(
                            $"{factory.Path}/_",
                            bytes,
                            factory.AclList,
                            CreateMode.PERSISTENT
                        )
                    );
                }

            }
            if(isRoot)
            {
                factory.RunOps().Wait();
            }
            return null;
        }
        throw new InvalidOperationException();
    }
    public override void Write(Utf8JsonWriter writer, ZkStub value, JsonSerializerOptions options)
    {
        if ((ZkJsonSerializer?)options.Converters.Where(c => c is ZkJsonSerializer).FirstOrDefault() is ZkJsonSerializer factory)
        {
            if (factory.IsReady)
            {
                string root = factory.Root.StartsWith("/") ? factory.Root[1..] : factory.Root;
                if (!string.IsNullOrEmpty(root))
                {
                    factory.PushPathComponent(root);
                }
            }
            DataResult dr = factory.ZooKeeper.getDataAsync(factory.Path).Result;
            bool isDouble = false;
            JsonValueKind jsonValueKind = JsonValueKind.Undefined;
            int valueKind = (int)BytesToLong(dr.Data);
            if((valueKind & DOUBLE) == DOUBLE)
            {
                valueKind ^= DOUBLE;
                isDouble = true;
            }
            jsonValueKind = Enum.GetValues<JsonValueKind>()[valueKind];

            if(jsonValueKind is JsonValueKind.Object)
            {
                writer.WriteStartObject();
                ChildrenResult cr = factory.ZooKeeper.getChildrenAsync(factory.Path).Result;
                foreach(string child in cr.Children)
                {
                    factory.PushPathComponent(child);
                    writer.WritePropertyName(child);
                    Write(writer, value, options);
                    factory.PopPathComponent();
                }
                writer.WriteEndObject();
            }
            else if(jsonValueKind is JsonValueKind.Array)
            {
                writer.WriteStartArray();
                ChildrenResult cr = factory.ZooKeeper.getChildrenAsync(factory.Path).Result;
                foreach (string child in cr.Children.OrderBy(v => v))
                {
                    factory.PushPathComponent(child);
                    Write(writer, value, options);
                    factory.PopPathComponent();
                }
                writer.WriteEndArray();
            }
            else if(jsonValueKind is JsonValueKind.String)
            {
                dr = factory.ZooKeeper.getDataAsync($"{factory.Path}/_").Result;
                writer.WriteStringValue(BytesToString(dr.Data));
            }
            else if(jsonValueKind is JsonValueKind.Number)
            {
                dr = factory.ZooKeeper.getDataAsync($"{factory.Path}/_").Result;
                if (isDouble)
                {
                    writer.WriteNumberValue(BytesToDouble(dr.Data));
                }
                else
                {
                    writer.WriteNumberValue(BytesToLong(dr.Data));
                }
            }
            else if (jsonValueKind is JsonValueKind.False)
            {
                writer.WriteBooleanValue(false);
            }
            else if (jsonValueKind is JsonValueKind.True)
            {
                writer.WriteBooleanValue(true);
            }
            else if (jsonValueKind is JsonValueKind.Null)
            {
                writer.WriteNullValue();
            }

            return;
        }
        throw new InvalidOperationException();
    }
    private object FromBytes(byte[] bytes, Func<BinaryReader, object> func)
    {
        _memoryStream.SetLength(0);
        _memoryStream.Write(bytes);
        _memoryStream.Position = 0;
        return func(_binReader);
    }
    private double BytesToDouble(byte[] bytes) => (double)FromBytes(bytes, br => br.ReadDouble());
    private long BytesToLong(byte[] bytes) => (long)FromBytes(bytes, br => br.ReadInt64());
    private string BytesToString(byte[] bytes) => (string)FromBytes(bytes, br => br.ReadString());
    private byte[] ToBytes(Action<BinaryWriter> action)
    {
        _memoryStream.SetLength(0);
        action(_binWriter);
        _binWriter.Flush();
        return _memoryStream.ToArray();
    }
    private byte[] ToBytes(string value) => ToBytes(bw => bw.Write((string)value));
    private byte[] ToBytes(long value) => ToBytes(bw => bw.Write((long)value));
    private byte[] ToBytes(double value) => ToBytes(bw => bw.Write((double)value));
    private static async Task Delete(ZkJsonSerializer factory, string path)
    {
        if (await factory.ZooKeeper.existsAsync(path) is Stat stat)
        {
            if (stat.getNumChildren() > 0 && await factory.ZooKeeper.getChildrenAsync(path) is ChildrenResult childrenResult)
            {
                foreach (var child in childrenResult.Children)
                {
                    await Delete(factory, manySlashes.Replace($"{path}/{child}", "/"));
                }
            }
            factory.AddOp(Op.delete(path));
        }
    }

}
