using Net.Leksi.ZkJson;
using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System.Text.Json;
using ZkJsonDemo;

const string s_successUpdate = "Requested subtree was updated successfully!";
const string s_successRead = "\nRequested subtree was read successfully!";
const string s_successDelete = "Requested subtree was deleted successfully!";
const string s_connecting = "Connecting to ZooKeeper '{0}' ...";
const string s_zkState = "ZooKeeper is {0}";
const string s_failed = "failed!";
const string s_pathNotExists = "Requested subtree does not exist!";

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
    Console.WriteLine(s_connecting, options.ConnectionString);
    zk = new ZooKeeper(options.ConnectionString, options.Timeout, new ZKWatcher(mres));
    mres.Wait(options.Timeout);

    Console.WriteLine(s_zkState, zk.getState());

    if (zk.getState() is not ZooKeeper.States.CONNECTED)
    {
        Console.WriteLine(s_failed);
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
        await JsonSerializer.DeserializeAsync<ZkStub>(options.Reader, serializerOptions);
        Console.WriteLine(s_successUpdate);
    }
    else if (options.Writer is { })
    {
        if (await zk.existsAsync("/") is Stat stat)
        {

            await JsonSerializer.SerializeAsync(options.Writer, ZkStub.Instance, serializerOptions);
            Console.WriteLine(s_successRead);
            factory.Reset();
            JsonElement jsonElement = JsonSerializer.SerializeToElement(ZkStub.Instance, serializerOptions);
            Console.WriteLine(jsonElement);
        }
        else
        {
            Console.WriteLine(s_pathNotExists);
            Environment.Exit(1);
            return;
        }
    }
    else if (options.Delete)
    {
        if (await zk.existsAsync("/") is Stat stat)
        {
            await factory.DeleteAsync();
            Console.WriteLine(s_successDelete);
        }
        else
        {
            Console.WriteLine(s_pathNotExists);
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
