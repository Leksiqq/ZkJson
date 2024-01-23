namespace Net.Leksi.ZkJson;

public class ZkNode
{
    public static ZkNode Node { get; private set; } = new();
    private ZkNode() { }
}
