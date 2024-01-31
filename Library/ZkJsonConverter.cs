using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace Net.Leksi.ZkJson;

internal class ZkJsonConverter : JsonConverter<ZkStub>
{
    private readonly ZkJsonSerializer _factory;
    internal static int DOUBLE = 0x10000;
    internal ZkJsonConverter(ZkJsonSerializer factory)
    {
        _factory = factory;
    }
    public override ZkStub? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        bool isRoot = false;
        if (_factory.IsReady)
        {
            isRoot = true;
            string root = _factory.Root.StartsWith("/") ? _factory.Root[1..] : _factory.Root;
            if (!string.IsNullOrEmpty(root))
            {
                _factory.PushPathComponent(root);
            }
            if (_factory.Action is ZkAction.Replace || _factory.Deletion)
            {
                Delete(_factory, _factory.Path).Wait();
            }
            if (_factory.Deletion)
            {
                while (reader.Read()) { }
                _factory.RunOps().Wait();
                return null;
            }
        }
        if (reader.TokenType is JsonTokenType.StartObject)
        {
            if (_factory.Action is ZkAction.Replace)
            {
                _factory.AddOp(Op.create(_factory.Path, _factory.ToBytes((long)JsonValueKind.Object), _factory.AclList, CreateMode.PERSISTENT));
            }
            else
            {
                _factory.AddOp(Op.setData(_factory.Path, _factory.ToBytes((long)JsonValueKind.Object)));
            }
            while (reader.Read() && reader.TokenType is not JsonTokenType.EndObject)
            {
                if (reader.TokenType is not JsonTokenType.PropertyName)
                {
                    throw new JsonException("Property name missed!");
                }
                string propertyName = reader.GetString() ?? throw new JsonException("Property name missed!");
                if (!reader.Read())
                {
                    throw new JsonException("Property value missed!");
                }
                _factory.PushPathComponent(propertyName);
                Read(ref reader, typeToConvert, options);
                _factory.PopPathComponent();
            }
        }
        else if (reader.TokenType is JsonTokenType.StartArray)
        {
            if (_factory.Action is ZkAction.Update)
            {
                throw new ZkJsonException("Cannot update array!") { HResult = ZkJsonException.CannotUpdateArray };
            }
            if (_factory.Action is ZkAction.Replace)
            {
                _factory.AddOp(Op.create(_factory.Path, _factory.ToBytes((long)JsonValueKind.Array), _factory.AclList, CreateMode.PERSISTENT));
            }
            else
            {
                _factory.AddOp(Op.setData(_factory.Path, _factory.ToBytes((long)JsonValueKind.Array)));
            }
            long pos = 0;
            while (reader.Read())
            {
                if (reader.TokenType is JsonTokenType.EndArray)
                {
                    break;
                }
                _factory.PushPathComponent(pos.ToString("D18"));
                Read(ref reader, typeToConvert, options);
                _factory.PopPathComponent();
                ++pos;
            }
        }
        else
        {
            byte[]? bytes = null;
            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                    if (_factory.Action is ZkAction.Replace)
                    {
                        _factory.AddOp(Op.create(_factory.Path, _factory.ToBytes((long)JsonValueKind.String), _factory.AclList, CreateMode.PERSISTENT));
                    }
                    else
                    {
                        _factory.AddOp(Op.setData(_factory.Path, _factory.ToBytes((long)JsonValueKind.String)));
                    }
                    bytes = _factory.ToBytes(reader.GetString()!);
                    break;
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out long longValue))
                    {
                        if (_factory.Action is ZkAction.Replace)
                        {
                            _factory.AddOp(Op.create(_factory.Path, _factory.ToBytes((long)JsonValueKind.Number), _factory.AclList, CreateMode.PERSISTENT));
                        }
                        else
                        {
                            _factory.AddOp(Op.setData(_factory.Path, _factory.ToBytes((long)JsonValueKind.Number)));
                        }
                        bytes = _factory.ToBytes(longValue);
                    }
                    else if (reader.TryGetDouble(out double doubleValue))
                    {
                        if (_factory.Action is ZkAction.Replace)
                        {
                            _factory.AddOp(Op.create(_factory.Path, _factory.ToBytes((int)JsonValueKind.Number | DOUBLE), _factory.AclList, CreateMode.PERSISTENT));
                        }
                        else
                        {
                            _factory.AddOp(Op.setData(_factory.Path, _factory.ToBytes((long)JsonValueKind.Number | DOUBLE)));
                        }
                        bytes = _factory.ToBytes(doubleValue);
                    }
                    break;
                case JsonTokenType.True:
                    if (_factory.Action is ZkAction.Replace)
                    {
                        _factory.AddOp(Op.create(_factory.Path, _factory.ToBytes((int)JsonValueKind.True), _factory.AclList, CreateMode.PERSISTENT));
                    }
                    else
                    {
                        _factory.AddOp(Op.setData(_factory.Path, _factory.ToBytes((long)JsonValueKind.True)));
                    }
                    break;
                case JsonTokenType.False:
                    if (_factory.Action is ZkAction.Replace)
                    {
                        _factory.AddOp(Op.create(_factory.Path, _factory.ToBytes((int)JsonValueKind.False), _factory.AclList, CreateMode.PERSISTENT));
                    }
                    else
                    {
                        _factory.AddOp(Op.setData(_factory.Path, _factory.ToBytes((long)JsonValueKind.False)));
                    }
                    break;
                case JsonTokenType.Null:
                    if (_factory.Action is ZkAction.Update)
                    {
                        Delete(_factory, _factory.Path).Wait();
                    }
                    break;
            }
            if (bytes is { })
            {
                if (_factory.Action is ZkAction.Replace)
                {
                    _factory.AddOp(
                        Op.create(
                            ZkJsonSerializer.manySlashes.Replace($"{_factory.Path}/_", "/"),
                            bytes,
                            _factory.AclList,
                            CreateMode.PERSISTENT
                        )
                    );
                }
                else
                {
                    _factory.AddOp(
                        Op.setData(
                            ZkJsonSerializer.manySlashes.Replace($"{_factory.Path}/_", "/"),
                            bytes
                        )
                    );
                }
            }

        }
        if (isRoot)
        {
            _factory.RunOps().Wait();
        }
        return null;
    }
    public override void Write(Utf8JsonWriter writer, ZkStub value, JsonSerializerOptions options)
    {
        if (_factory.IsReady)
        {
            string root = _factory.Root.StartsWith("/") ? _factory.Root[1..] : _factory.Root;
            if (!string.IsNullOrEmpty(root))
            {
                _factory.PushPathComponent(root);
            }
        }
        DataResult dr = _factory.ZooKeeper.getDataAsync(_factory.Path).Result;
        bool isDouble = false;
        JsonValueKind jsonValueKind = JsonValueKind.Undefined;
        int valueKind = (int)_factory.BytesToLong(dr.Data);
        if ((valueKind & DOUBLE) == DOUBLE)
        {
            valueKind ^= DOUBLE;
            isDouble = true;
        }
        jsonValueKind = Enum.GetValues<JsonValueKind>()[valueKind];

        if (jsonValueKind is JsonValueKind.Object)
        {
            writer.WriteStartObject();
            ChildrenResult cr = _factory.ZooKeeper.getChildrenAsync(_factory.Path).Result;
            foreach (string child in cr.Children)
            {
                _factory.PushPathComponent(child);
                writer.WritePropertyName(child);
                Write(writer, value, options);
                _factory.PopPathComponent();
            }
            writer.WriteEndObject();
        }
        else if (jsonValueKind is JsonValueKind.Array)
        {
            writer.WriteStartArray();
            ChildrenResult cr = _factory.ZooKeeper.getChildrenAsync(_factory.Path).Result;
            foreach (string child in cr.Children.OrderBy(v => v))
            {
                _factory.PushPathComponent(child);
                Write(writer, value, options);
                _factory.PopPathComponent();
            }
            writer.WriteEndArray();
        }
        else if (jsonValueKind is JsonValueKind.String)
        {
            dr = _factory.ZooKeeper.getDataAsync(ZkJsonSerializer.manySlashes.Replace($"{_factory.Path}/_", "/")).Result;
            writer.WriteStringValue(_factory.BytesToString(dr.Data));
        }
        else if (jsonValueKind is JsonValueKind.Number)
        {
            dr = _factory.ZooKeeper.getDataAsync(ZkJsonSerializer.manySlashes.Replace($"{_factory.Path}/_", "/")).Result;
            if (isDouble)
            {
                writer.WriteNumberValue(_factory.BytesToDouble(dr.Data));
            }
            else
            {
                writer.WriteNumberValue(_factory.BytesToLong(dr.Data));
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
    private static async Task Delete(ZkJsonSerializer factory, string path)
    {
        if (await factory.ZooKeeper.existsAsync(path) is Stat stat)
        {
            if (stat.getNumChildren() > 0 && await factory.ZooKeeper.getChildrenAsync(path) is ChildrenResult childrenResult)
            {
                foreach (var child in childrenResult.Children)
                {
                    await Delete(factory, ZkJsonSerializer.manySlashes.Replace($"{path}/{child}", "/"));
                }
            }
            factory.AddOp(Op.delete(path));
        }
    }

}
