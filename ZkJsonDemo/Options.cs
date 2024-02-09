namespace ZkJsonDemo;

internal class Options
{
    private const string s_usage = @$"Usage:

{{0}} [-c <zookeeper connection string>] [-t <timeout ms>] [-p <path>] [-u] -w [<file>|-]
or
{{0}} [-c <zookeeper connection string>] [-t <timeout ms>] [-p <path>] [-i <script prefix>] -r [<file>|-]
or
{{0}} [-c <zookeeper connection string>] [-t <timeout ms>] [-p <path>] -d

where

    -c <zookeeper connection string>    ZooKeeper connection string
    -t <timeout ms>                     Connection timeout, ms (default 1000)
    -p <path>                           path at ZooKeeper to operate with
    -u                                  Update data
    -w -                                Write data from console
    -w <file>                           Write data from file <file>
    -i <script prefix>                  Read data incremental
    -r -                                Read data to console
    -r <file>                           Read data to file <file>
    -d                                  Delete data
";

internal string ConnectionString { get; private set; } = "localhost:2181";
    internal Stream? Reader { get; private set; } = null;
    internal Stream? Writer { get; private set; } = null;
    internal bool Delete { get; private set; } = false;
    internal bool Update { get; private set; } = false;
    internal string? BasePropertyName { get; private set; } = null;
    internal int Timeout { get; private set; } = 1000;
    internal string Path { get; private set; } = "/";
    private Options() { }
    internal static Options? Create(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine(s_usage, System.IO.Path.GetFileName(Environment.ProcessPath));
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
                        if (options.Update)
                        {
                            CannotBothReadAndUpdate();
                            return null;
                        }
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
                        if (options.BasePropertyName is { })
                        {
                            CannotReadIncremental();
                            return null;
                        }
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
                    if (arg == "-u")
                    {
                        if (options.Delete)
                        {
                            CannotBothDeleteAndUpdate();
                            return null;
                        }
                        if (options.Writer is { })
                        {
                            CannotBothReadAndUpdate();
                            return null;
                        }
                        options.Update = true;
                        break;
                    }
                    if (arg == "-i")
                    {
                        if (options.Delete)
                        {
                            CannotDeleteIncremental();
                            return null;
                        }
                        if (options.Reader is { })
                        {
                            CannotReadIncremental();
                            return null;
                        }
                        waiting = Waiting.BasePropertyName;
                        break;
                    }
                    if (arg == "-d")
                    {
                        if (options.Update)
                        {
                            CannotBothDeleteAndUpdate();
                            return null;
                        }
                        if (options.BasePropertyName is { })
                        {
                            CannotDeleteIncremental();
                            return null;
                        }
                        if (options.Delete || options.Reader is { } || options.Writer is { })
                        {
                            ExtraKeysFound();
                            return null;
                        }
                        options.Delete = true;
                        break;
                    }
                    if(arg == "-p")
                    {
                        waiting = Waiting.Path;
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
                case Waiting.BasePropertyName:
                    options.BasePropertyName = arg;
                    waiting = Waiting.None;
                    break;
                case Waiting.Path:
                    options.Path = arg;
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

    private static void CannotReadIncremental()
    {
        Console.WriteLine($"Command line cannot have -w and -i keys together!");
    }

    private static void CannotDeleteIncremental()
    {
        Console.WriteLine($"Command line cannot have -d and -i keys together!");
    }

    private static void CannotBothReadAndUpdate()
    {
        Console.WriteLine($"Command line cannot have -r and -u keys together!");
        Environment.Exit(1);
    }

    private static void CannotBothDeleteAndUpdate()
    {
        Console.WriteLine($"Command line cannot have -d and -u keys together!");
        Environment.Exit(1);
    }

    private static void ExtraKeysFound()
    {
        Console.WriteLine($"Command line must have one -r or -w or -d key!");
        Environment.Exit(1);
    }

}
