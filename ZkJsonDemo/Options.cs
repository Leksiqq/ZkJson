namespace ZkJsonDemo;

internal class Options
{
    private const string s_usage = @$"Usage:

{{0}} [-c <zookeeper connection string>] [-t <timeout ms>] [-r|-w] [<file>|-]
or
{{0}} [-c <zookeeper connection string>] [-t <timeout ms>] -d

where

    -c <zookeeper connection string>    ZooKeeper connection string ""addr1:port1,addr2:port2,...[/optional_chroot]""
    -t <timeout ms>                     Connection timeout, ms (default 1000)
    -r -                                Read data to console
    -r <file>                           Read data to file <file>
    -w -                                Write data from console
    -w <file>                           Write data from file <file>
    -d                                  Delete data
";

internal string ConnectionString { get; private set; } = "localhost:2181";
    internal Stream? Reader { get; private set; } = null;
    internal Stream? Writer { get; private set; } = null;
    internal bool Delete { get; private set; } = false;
    internal int Timeout { get; private set; } = 1000;
    private Options() { }
    internal static Options? Create(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine(s_usage, Path.GetFileName(Environment.ProcessPath));
            return null;
        }

        Options options = new Options();
        Waiting waiting = Waiting.None;

        foreach (var arg in args)
        {
            switch (waiting)
            {
                case Waiting.None:
                    if (arg == "-c")
                    {
                        waiting = Waiting.ConnectionString;
                        break;
                    }
                    if (arg == "-r")
                    {
                        if (options.Delete || options.Reader is { } || options.Writer is { })
                        {
                            ExtraKeysFound();
                            return null;
                        }
                        waiting = Waiting.Read;
                        break;
                    }
                    if (arg == "-w")
                    {
                        if (options.Delete || options.Reader is { } || options.Writer is { })
                        {
                            ExtraKeysFound();
                            return null;
                        }
                        waiting = Waiting.Write;
                        break;
                    }
                    if (arg == "-t")
                    {
                        waiting = Waiting.Timeout;
                        break;
                    }
                    if (arg == "-d")
                    {
                        if (options.Delete || options.Reader is { } || options.Writer is { })
                        {
                            ExtraKeysFound();
                            return null;
                        }
                        options.Delete = true;
                        break;
                    }
                    Console.WriteLine($"Invalid key: {arg}");
                    return null;
                case Waiting.ConnectionString:
                    options.ConnectionString = arg;
                    waiting = Waiting.None;
                    break;
                case Waiting.Timeout:
                    options.Timeout = int.Parse(arg);
                    waiting = Waiting.None;
                    break;
                case Waiting.Read:
                    if (arg == "-")
                    {
                        options.Writer = Console.OpenStandardOutput();
                    }
                    else
                    {
                        options.Writer = new FileStream(arg, FileMode.Create);
                    }
                    waiting = Waiting.None;
                    break;
                case Waiting.Write:
                    if (arg == "-")
                    {
                        options.Reader = Console.OpenStandardInput();
                    }
                    else
                    {
                        options.Reader = new FileStream(arg, FileMode.Open);
                    }
                    waiting = Waiting.None;
                    break;
            }
        }

        if (options.Reader is null && options.Writer is null && !options.Delete)
        {
            ExtraKeysFound();
            return null;
        }


        return options;
    }
    private static void ExtraKeysFound()
    {
        Console.WriteLine($"Command line must have one -r or -w or -d key!");
        Environment.Exit(1);
    }

}
