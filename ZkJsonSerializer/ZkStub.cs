namespace Net.Leksi.ZkJson;

public class ZkStub

{
    public static ZkStub Instance { get; private set; } = new();
    private ZkStub() { }
}
