using Net.Leksi.ZkJson;
using org.apache.zookeeper;
using System.Text.Json;

namespace TestProject1;

public class Tests
{
    private const string s_connectionString = "vm-kafka:2181/TestProject1";
    [Test]
    public void Test1()
    {
        Random rnd = new Random();
        string[] names = ["order", "subrequest", "surprize", "image"];
        string[] statuses = ["done", "pending", "running"];
        var query = Enumerable.Range(0, 4).Select(i => new
        {
            pos = i,
            name = names[rnd.Next(0, names.Length)],
            longValue = rnd.NextInt64(),
            doubleValue = rnd.NextDouble(),
            status = statuses[rnd.Next(0, statuses.Length)],
            watched = rnd.Next(0, 2) == 0,
            timestamp = new DateTime(rnd.NextInt64(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks)),
        });
        ManualResetEventSlim mres = new(false);
        ZooKeeper zk = new(s_connectionString, 1000, new MyWatcher(mres));
        mres.Wait();

        ZkJson zkJson = new()
        {
            ZooKeeper = zk,
        };
        JsonSerializerOptions options = new()
        {
            WriteIndented = true,
        };
        options.Converters.Add(zkJson);
        JsonSerializer.Deserialize<ZkStub>(JsonSerializer.SerializeToElement(query, options), options);
        zkJson.Reset();
        MemoryStream ms = new();
        JsonSerializer.Serialize(ms, ZkStub.Instance, options);
        ms.Flush();
        ms.Position = 0;
        Console.WriteLine(new StreamReader(ms).ReadToEnd());
    }
    class MyWatcher(ManualResetEventSlim mres) : Watcher
    {
        public override async Task process(WatchedEvent @event)
        {
            mres.Set();
            await Task.CompletedTask;
        }
    }
}