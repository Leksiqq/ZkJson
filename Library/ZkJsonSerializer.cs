#define VERBOSE
using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System.Collections.Specialized;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using static org.apache.zookeeper.ZooDefs;

namespace Net.Leksi.ZkJson;

public class ZkJsonSerializer : JsonConverterFactory
{
    private readonly List<string> _path = [];
    private readonly List<Op> _ops = [];
    private readonly MemoryStream _memoryStream = new();
    private BinaryWriter? _binWriter = null;
    private BinaryReader? _binReader = null;

    internal static readonly Regex manySlashes = new("/{2,}");

    public ZooKeeper ZooKeeper { get; set; } = null!;
    public string Root { get; set; } = "/";
    public List<ACL> AclList { get; set; } = [new ACL((int)Perms.ALL, Ids.ANYONE_ID_UNSAFE)];
    public ZkAction Action { get; set; } = ZkAction.Replace;
    internal string Path => $"/{string.Join('/', _path)}";
    internal bool IsReady { get; private set; } = true;
    internal bool Deletion { get; private set; } = false;
    public void Reset()
    {
        _ops.Clear();
        _path.Clear();
        IsReady = true;
        Deletion = false;
    }
    public void Reset(string newRoot)
    {
        Reset();
        Root = newRoot;
    }
    public async Task DeleteAsync()
    {
        Deletion = true;
        MemoryStream ms = new(Encoding.ASCII.GetBytes("[]"));
        JsonSerializerOptions options = new();
        options.Converters.Add(this);
        await JsonSerializer.DeserializeAsync<ZkStub>(ms, options);
        Reset();
    }
    public async Task<bool> RootExists()
    {
        return await ZooKeeper.existsAsync(Root) is { };
    }
    public async Task CreateRoot()
    {
        string[] parts = Root.Split('/', StringSplitOptions.RemoveEmptyEntries);
        int i = parts.Length;
        for (; i > 0 ; --i)
        {
            string probe = $"/{string.Join('/', parts.Take(i))}";
            if(await ZooKeeper.existsAsync(probe) is { })
            {
                break;
            }
        }
        for (++i; i <= parts.Length; ++i)
        {
            string probe = $"/{string.Join('/', parts.Take(i))}";
            if (await ZooKeeper.existsAsync(probe) is null)
            {
                await ZooKeeper.createAsync(probe, [], AclList, CreateMode.PERSISTENT);
            }
        }
    }
    public JsonElement IncrementalSerialize(string basePropertyName)
    {
        HashSet<string> usedBases = [];
        IncrementalHolder res = WalkAround(usedBases, this, basePropertyName, Root);
        JsonSerializerOptions op = new();
        op.Converters.Add(this);
        return JsonSerializer.SerializeToElement(res, op);
    }

    public override bool CanConvert(Type typeToConvert)
    {
        return typeof(ZkStub).IsAssignableFrom(typeToConvert) || typeof(IncrementalHolder).IsAssignableFrom(typeToConvert);
    }
    public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        if (typeof(ZkStub).IsAssignableFrom(typeToConvert))
        {
            return new ZkJsonConverter(this);
        }
        return new IncrementalHolderJsonConverter();
    }
    internal async Task<List<OpResult>> RunOps()
    {
        return await ZooKeeper.multiAsync(_ops);
    }
    internal void PushPathComponent(string component)
    {
        IsReady = false;
        _path.Add(component);
    }
    internal void PopPathComponent()
    {
        _path.RemoveAt(_path.Count - 1);
    }
    internal void AddOp(Op op)
    {
        _ops.Add(op);
    }
    internal double BytesToDouble(byte[] bytes) => (double)FromBytes(bytes, br => br.ReadDouble());
    internal long BytesToLong(byte[] bytes) => (long)FromBytes(bytes, br => br.ReadInt64());
    internal string BytesToString(byte[] bytes) => (string)FromBytes(bytes, br => br.ReadString());
    internal byte[] ToBytes(string value) => ToBytes(bw => bw.Write((string)value));
    internal byte[] ToBytes(long value) => ToBytes(bw => bw.Write((long)value));
    internal byte[] ToBytes(double value) => ToBytes(bw => bw.Write((double)value));
    private object FromBytes(byte[] bytes, Func<BinaryReader, object> func)
    {
        if(_binReader is null)
        {
            _binReader = new BinaryReader(_memoryStream);
        }
        _memoryStream.SetLength(0);
        _memoryStream.Write(bytes);
        _memoryStream.Position = 0;
        return func(_binReader);
    }
    private byte[] ToBytes(Action<BinaryWriter> action)
    {
        if (_binWriter is null)
        {
            _binWriter = new BinaryWriter(_memoryStream);
        }
        _memoryStream.SetLength(0);
        action(_binWriter);
        _binWriter.Flush();
        return _memoryStream.ToArray();
    }
    private IncrementalHolder WalkAround(HashSet<string> usedBases, ZkJsonSerializer zkJsonSerializer, string basePropertyName, string path)
    {
        JsonSerializerOptions op = new();
        op.Converters.Add(zkJsonSerializer);
        JsonElement el = JsonSerializer.SerializeToElement(ZkStub.Instance, op);
        return WalkElement(el, usedBases, basePropertyName, path);
    }
    private IncrementalHolder WalkElement(JsonElement el, HashSet<string> usedBases, string basePropertyName, string path)
    {
        IncrementalHolder res;

        if (el.ValueKind is JsonValueKind.Object)
        {
            Dictionary<string, IncrementalHolder> dict = [];
            res = new IncrementalHolder { _value = dict };

            foreach (JsonProperty it in el.EnumerateObject())
            {
                IncrementalHolder cur;
                if (
                    it.Name == basePropertyName 
                    && (
                        it.Value.ValueKind is JsonValueKind.String
                        || it.Value.ValueKind is JsonValueKind.Array
                    )
                )
                {
                    string[] bases = (it.Value.ValueKind is JsonValueKind.String 
                        ? new string[] { it.Value.GetString()! } 
                        :  it.Value.EnumerateArray().Select(v => v.GetString()).ToArray())!;
                    foreach(string s in bases)
                    {
                        string refPath = manySlashes.Replace(new Uri(new Uri($"http://localhost{path}"), s).AbsolutePath, "/");
                        if (!usedBases.Add(refPath))
                        {
                            throw new ZkJsonException("Loop detected!") { HResult = ZkJsonException.IncrementalLoop };
                        }
                        ZkJsonSerializer serializer = new()
                        {
                            ZooKeeper = ZooKeeper,
                            Root = refPath,
                            AclList = AclList,
                        };
                        cur = WalkAround(usedBases, serializer, basePropertyName, path);
                        if (cur._value is Dictionary<string, IncrementalHolder> dict1)
                        {
                            foreach (var en in dict1)
                            {
                                if (!dict.ContainsKey(en.Key))
                                {
                                    dict.Add(en.Key, en.Value);
                                }
                            }
                        }
                        else
                        {
                            throw new ZkJsonException("Loop detected!") { HResult = ZkJsonException.IncrementalNotObject };
                        }
                        usedBases.Remove(refPath);
                    }
                }
                else
                {
                    string name = it.Name;
                    if (!name.StartsWith('-'))
                    {
                        cur = WalkElement(it.Value, usedBases, basePropertyName, $"{path}/{name}");
                    }
                    else
                    {
                        name = name[1..];
                        cur = new IncrementalHolder();
                    }
                    if (dict.ContainsKey(name))
                    {
                        dict[name] = cur;
                    }
                    else
                    {
                        dict.Add(name, cur);
                    }
                }
            }
        }
        else if (el.ValueKind is JsonValueKind.Array)
        {
            List<IncrementalHolder> list = [];
            res = new IncrementalHolder { _value = list };
            int pos = 0;
            foreach (JsonElement it in el.EnumerateArray())
            {
                list.Add(WalkElement(it, usedBases, basePropertyName, $"{path}/{pos}"));
                ++pos;
            }
        }
        else
        {
            res = new IncrementalHolder { _value = el };
        }
        return res;
    }
}
