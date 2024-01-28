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
                if(factory.Action is ZkAction.Replace || factory.Deletion)
                {
                    Delete(factory, factory.Path).Wait();
                }
                if (factory.Deletion)
                {
                    while (reader.Read()) { }
                    factory.RunOps().Wait();
                    return null;
                }
            }
            if (reader.TokenType is JsonTokenType.StartObject)
            {
                if(factory.Action is ZkAction.Replace)
                {
                    factory.AddOp(Op.create(factory.Path, factory.ToBytes((long)JsonValueKind.Object), factory.AclList, CreateMode.PERSISTENT));
                }
                else
                {
                    factory.AddOp(Op.setData(factory.Path, factory.ToBytes((long)JsonValueKind.Object)));
                }
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
                if(factory.Action is ZkAction.Update)
                {
                    throw new ZkJsonException("Cannot update array!") { HResult = ZkJsonException.CannotUpdateArray };
                }
                if (factory.Action is ZkAction.Replace)
                {
                    factory.AddOp(Op.create(factory.Path, factory.ToBytes((long)JsonValueKind.Array), factory.AclList, CreateMode.PERSISTENT));
                }
                else
                {
                    factory.AddOp(Op.setData(factory.Path, factory.ToBytes((long)JsonValueKind.Array)));
                }
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
                        if (factory.Action is ZkAction.Replace)
                        {
                            factory.AddOp(Op.create(factory.Path, factory.ToBytes((long)JsonValueKind.String), factory.AclList, CreateMode.PERSISTENT));
                        }
                        else
                        {
                            factory.AddOp(Op.setData(factory.Path, factory.ToBytes((long)JsonValueKind.String)));
                        }
                        bytes = factory.ToBytes(reader.GetString()!);
                        break;
                    case JsonTokenType.Number:
                        if (reader.TryGetInt64(out long longValue))
                        {
                            if (factory.Action is ZkAction.Replace)
                            {
                                factory.AddOp(Op.create(factory.Path, factory.ToBytes((long)JsonValueKind.Number), factory.AclList, CreateMode.PERSISTENT));
                            }
                            else
                            {
                                factory.AddOp(Op.setData(factory.Path, factory.ToBytes((long)JsonValueKind.Number)));
                            }
                            bytes = factory.ToBytes(longValue);
                        }
                        else if (reader.TryGetDouble(out double doubleValue))
                        {
                            if (factory.Action is ZkAction.Replace)
                            {
                                factory.AddOp(Op.create(factory.Path, factory.ToBytes((int)JsonValueKind.Number | DOUBLE), factory.AclList, CreateMode.PERSISTENT));
                            }
                            else
                            {
                                factory.AddOp(Op.setData(factory.Path, factory.ToBytes((long)JsonValueKind.Number | DOUBLE)));
                            }
                            bytes = factory.ToBytes(doubleValue);
                        }
                        break;
                    case JsonTokenType.True:
                        if (factory.Action is ZkAction.Replace)
                        {
                            factory.AddOp(Op.create(factory.Path, factory.ToBytes((int)JsonValueKind.True), factory.AclList, CreateMode.PERSISTENT));
                        }
                        else
                        {
                            factory.AddOp(Op.setData(factory.Path, factory.ToBytes((long)JsonValueKind.True)));
                        }
                        break;
                    case JsonTokenType.False:
                        if (factory.Action is ZkAction.Replace)
                        {
                            factory.AddOp(Op.create(factory.Path, factory.ToBytes((int)JsonValueKind.False), factory.AclList, CreateMode.PERSISTENT));
                        }
                        else
                        {
                            factory.AddOp(Op.setData(factory.Path, factory.ToBytes((long)JsonValueKind.False)));
                        }
                        break;
                    case JsonTokenType.Null:
                        if (factory.Action is ZkAction.Update)
                        {
                            Delete(factory, factory.Path).Wait();
                        }
                        break;
                }
                if (bytes is { })
                {
                    if (factory.Action is ZkAction.Replace)
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
                    else
                    {
                        factory.AddOp(
                            Op.setData(
                                $"{factory.Path}/_",
                                bytes
                            )
                        );
                    }
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
            int valueKind = (int)factory.BytesToLong(dr.Data);
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
                writer.WriteStringValue(factory.BytesToString(dr.Data));
            }
            else if(jsonValueKind is JsonValueKind.Number)
            {
                dr = factory.ZooKeeper.getDataAsync($"{factory.Path}/_").Result;
                if (isDouble)
                {
                    writer.WriteNumberValue(factory.BytesToDouble(dr.Data));
                }
                else
                {
                    writer.WriteNumberValue(factory.BytesToLong(dr.Data));
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
