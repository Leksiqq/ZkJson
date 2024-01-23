using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System.Text.Json;
using System.Text.Json.Serialization;
using static org.apache.zookeeper.ZooDefs;

namespace Net.Leksi.ZkJson;

public class ZkJson : JsonConverterFactory
{
    private readonly List<string> _path = [];
    private readonly List<Op> _ops = [];
    public ZooKeeper ZooKeeper { get; set; } = null!;
    public string Root { get; set; } = "/";
    public List<ACL> AclList { get; set; } = [new ACL((int)Perms.ALL, Ids.ANYONE_ID_UNSAFE)];
    internal string Path => $"/{string.Join('/', _path)}";
    internal bool IsReady { get; private set; } = true;
    internal int OpsCount => _ops.Count;
    public void Reset()
    {
        _ops.Clear();
        _path.Clear();
        IsReady = true;
    }
    public void PushPathComponent(string component)
    {
        IsReady = false;
        _path.Add(component);
    }
    public void PopPathComponent()
    {
        _path.RemoveAt(_path.Count - 1);
    }
    public void AddOp(Op op)
    {
        _ops.Add(op);
    }
    public override bool CanConvert(Type typeToConvert)
    {
        return typeof(ZkNode).IsAssignableFrom(typeToConvert);
    }
    public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        return new ZkJsonConverter();
    }
    internal void TruncOps(int count)
    {
        _ops.RemoveRange(count, _ops.Count - count);
    }
    internal async Task<List<OpResult>> RunOps()
    {
        return await ZooKeeper.multiAsync(_ops);
    }
}
