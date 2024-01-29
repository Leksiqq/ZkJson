using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using static org.apache.zookeeper.ZooDefs;

namespace Net.Leksi.ZkJson;

public class ZkJsonSerializer : JsonConverterFactory
{
    private readonly List<string> _path = [];
    private readonly List<Op> _ops = [];
    private readonly MemoryStream _memoryStream = new();
    private BinaryWriter? _binWriter = null;
    private BinaryReader? _binReader = null;

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
    public override bool CanConvert(Type typeToConvert)
    {
        return typeof(ZkStub).IsAssignableFrom(typeToConvert);
    }
    public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        return new ZkJsonConverter();
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
}
