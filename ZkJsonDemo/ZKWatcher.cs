using org.apache.zookeeper;
namespace ZkJsonDemo;

class ZKWatcher(ManualResetEventSlim mres) : Watcher
{
    public override async Task process(WatchedEvent @event)
    {
        mres.Set();
        await Task.CompletedTask;
    }
}

