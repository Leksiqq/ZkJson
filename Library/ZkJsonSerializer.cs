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
}
