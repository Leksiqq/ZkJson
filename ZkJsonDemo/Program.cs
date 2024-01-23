using org.apache.zookeeper.data;
using org.apache.zookeeper;
using ZkJsonDemo;
using Net.Leksi.ZkJson;
using System.Text.Json;
using System.Text;

Options? options = Options.Create(args);
if (options is null)
{
    Environment.Exit(1);
    return;
}

ZooKeeper zk = null!;
ManualResetEventSlim mres = new(false);

try
{
    Console.WriteLine("Connecting to ZooKeeper '{0}' ...", options.ConnectionString);
    zk = new ZooKeeper(options.ConnectionString, options.Timeout, new ZKWatcher(mres));
    mres.Wait(options.Timeout);

    Console.WriteLine("ZooKeeper is {0}", zk.getState());

    if (zk.getState() is not ZooKeeper.States.CONNECTED)
    {
        Console.WriteLine("failed!");
        Environment.Exit(1);
        return;
    }

    ZkJson factory = new() 
    {
        ZooKeeper = zk,
    };

    JsonSerializerOptions serializerOptions = new()
    {
        WriteIndented = true,
    };
    serializerOptions.Converters.Add(factory);

    if (options.Reader is { })
    {
        await JsonSerializer.DeserializeAsync<ZkNode>(options.Reader, serializerOptions);
        Console.WriteLine("Requested subtree was updated successfully!");
    }
    else if (options.Writer is { })
    {
        if (await zk.existsAsync("/") is Stat stat)
        {

            await JsonSerializer.SerializeAsync<ZkNode>(options.Writer, ZkNode.Node, serializerOptions);
            Console.WriteLine("\nRequested subtree was read successfully!");
        }
        else
        {
            Console.WriteLine("Requested subtree does not exist!");
            Environment.Exit(1);
            return;
        }
    }
    else if (options.Delete)
    {
        if (await zk.existsAsync("/") is Stat stat)
        {
            MemoryStream ms = new(Encoding.ASCII.GetBytes("[null]"));
            await JsonSerializer.DeserializeAsync<ZkNode>(ms, serializerOptions);
            Console.WriteLine("Requested subtree was deleted successfully!");
        }
        else
        {
            Console.WriteLine("Requested subtree does not exist!");
            Environment.Exit(1);
            return;
        }
    }
}
finally
{
    options.Reader?.Close();
    options.Writer?.Close();
}
