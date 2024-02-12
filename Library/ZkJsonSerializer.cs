using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using static org.apache.zookeeper.ZooDefs;

namespace Net.Leksi.ZkJson;

public class ZkJsonSerializer : JsonConverterFactory
{
    private readonly List<string> _path = [];
    private readonly List<Op> _ops = [];
    private readonly MemoryStream _memoryStream = new();
    private readonly JsonSerializerOptions _jsonSerializerOptions = new();
    private readonly JsonSerializerOptions _treeJsonSerializerOptions = new();
    private BinaryWriter? _binWriter = null;
    private BinaryReader? _binReader = null;

#pragma warning disable SYSLIB1045 // Преобразовать в "GeneratedRegexAttribute".
    private static readonly Regex manySlashes = new("/{2,}");
#pragma warning restore SYSLIB1045 // Преобразовать в "GeneratedRegexAttribute".

    public ZooKeeper ZooKeeper { get; set; } = null!;
    public string Root { get; set; } = "/";
    public List<ACL> AclList { get; set; } = [new ACL((int)Perms.ALL, Ids.ANYONE_ID_UNSAFE)];
    public ZkAction Action { get; set; } = ZkAction.Replace;
    internal string Path => $"/{string.Join('/', _path)}";
    internal bool IsReady { get; private set; } = true;
    internal bool Deletion { get; private set; } = false;
    public ZkJsonSerializer()
    {
        _jsonSerializerOptions.Converters.Add(this);
        _treeJsonSerializerOptions.Converters.Add(new TreeJsonConverter());
    }
    public void Reset()
    {
        _ops.Clear();
        _path.Clear();
        IsReady = true;
        Deletion = false;
    }
    public void Reset(string newRoot)
    {
        Reset();
        Root = newRoot;
    }
    public async Task DeleteAsync()
    {
        Deletion = true;
        MemoryStream ms = new(Encoding.ASCII.GetBytes("[]"));
        await JsonSerializer.DeserializeAsync<ZkStub>(ms, _jsonSerializerOptions);
        Reset();
    }
    public async Task<bool> RootExists()
    {
        return await ZooKeeper.existsAsync(Root) is { };
    }
    public async Task CreateRoot()
    {
        string[] parts = Root.Split('/', StringSplitOptions.RemoveEmptyEntries);
        int i = parts.Length;
        for (; i > 0 ; --i)
        {
            string probe = $"/{string.Join('/', parts.Take(i))}";
            if(await ZooKeeper.existsAsync(probe) is { })
            {
                break;
            }
        }
        for (++i; i <= parts.Length; ++i)
        {
            string probe = $"/{string.Join('/', parts.Take(i))}";
            if (await ZooKeeper.existsAsync(probe) is null)
            {
                await ZooKeeper.createAsync(probe, [], AclList, CreateMode.PERSISTENT);
            }
        }
    }
    public JsonElement IncrementalSerialize(string scriptPrefix)
    {
        JsonElement source = JsonSerializer.SerializeToElement(ZkStub.Instance, _jsonSerializerOptions);
        if (source.ValueKind is not JsonValueKind.Object && source.ValueKind is not JsonValueKind.Array)
        {
            return source;
        }
        IncremetalSerializeBag bag = new() { ScriptPrefix = $"{scriptPrefix}:" };
        string saveRoot = Root;
        Node root = ZkJsonSerializer.BuildGraph(source, Root, bag);
        ResolveReferences(root, bag);
        Root = saveRoot;
        RemovePrefix(Root, bag);
        return JsonSerializer.SerializeToElement(bag.Tree, _treeJsonSerializerOptions);
    }
    public override bool CanConvert(Type typeToConvert)
    {
        return typeof(ZkStub).IsAssignableFrom(typeToConvert);
    }
    public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        return new ZkJsonConverter(this);
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
    internal static string CollapseSlashes(string source)
    {
        return manySlashes.Replace(source, "/");
    }

    internal double BytesToDouble(byte[] bytes) => (double)FromBytes(bytes, br => br.ReadDouble());
    internal long BytesToLong(byte[] bytes) => (long)FromBytes(bytes, br => br.ReadInt64());
    internal string BytesToString(byte[] bytes) => (string)FromBytes(bytes, br => br.ReadString());
    internal byte[] ToBytes(string value) => ToBytes(bw => bw.Write((string)value));
    internal byte[] ToBytes(long value) => ToBytes(bw => bw.Write((long)value));
    internal byte[] ToBytes(double value) => ToBytes(bw => bw.Write((double)value));
    private object FromBytes(byte[] bytes, Func<BinaryReader, object> func)
    {
        _binReader ??= new BinaryReader(_memoryStream);
        _memoryStream.SetLength(0);
        _memoryStream.Write(bytes);
        _memoryStream.Position = 0;
        return func(_binReader);
    }
    private byte[] ToBytes(Action<BinaryWriter> action)
    {
        _binWriter ??= new BinaryWriter(_memoryStream);
        _memoryStream.SetLength(0);
        action(_binWriter);
        _binWriter.Flush();
        return _memoryStream.ToArray();
    }
    private static void RemovePrefix(string prefix, IncremetalSerializeBag bag)
    {
        int len = bag.Tree.Count;
        string prefix1 = CollapseSlashes($"{prefix}/");
        string[] keys = [.. bag.Tree.Keys];
        for (int i = 0; i < len; ++i)
        {
            JsonElement el = bag.Tree[keys[i]];
            bag.Tree.Remove(keys[i]);
            if (keys[i].StartsWith(prefix1))
            {
                string newPath = $"/{keys[i][prefix1.Length..]}";
                bag.Tree.Add(newPath, el);
            }
        }
    }
    private void ResolveReferences(Node node, IncremetalSerializeBag bag)
    {
        int result = Dfm(node, node.Path, bag);
        if (result > 0)
        {
            ZkJsonException ex;
            if (result == ZkJsonException.IncrementalCycle)
            {
                ex = new ZkJsonException("A cycle was detected!") { HResult = result };
                List<string> cycle = [];
                while (bag.Stack.Count > 0)
                {
                    cycle.Add(bag.Stack.Pop());
                    if (cycle.Count > 1 && cycle.Last() == cycle.First())
                    {
                        break;
                    }
                }
                cycle.Reverse();
                ex.Data.Add(nameof(ZkJsonException.IncrementalCycle), cycle);
            }
            else
            {
                ex = new ZkJsonException("Unknown");
            }
            throw ex;
        }
        foreach(string refPath in bag.Values.Keys)
        {
            Reset(refPath);
            JsonElement term = JsonSerializer.SerializeToElement(ZkStub.Instance, _jsonSerializerOptions);
            foreach (string name in bag.Values[refPath])
            {
                bag.Tree[name] = term;
            }
        }
        foreach(string path in bag.Evals)
        {
            if (
                bag.Tree[path] is JsonElement el 
                && el.ValueKind is JsonValueKind.String 
                && el.GetString() is string s
            )
            {
                StringBuilder sb = new();
                int pos = 0;
                for (Match evalMatch = bag.EvalTmpValue!.Match(s[pos..]); evalMatch.Success; evalMatch = bag.EvalTmpValue!.Match(s[pos..]))
                {
                    string refPath = CollapseSlashes(new Uri(new Uri($"http://localhost{path}"), evalMatch.Value).AbsolutePath);
                    sb.Append(s, pos, evalMatch.Groups[0].Index).Append(bag.Tree[refPath]);
                    pos += evalMatch.Groups[0].Index + evalMatch.Length;
                    bag.Tree.Remove(refPath);
                }
                sb.Append(s, pos, s.Length - pos);
                bag.Tree[path] = JsonSerializer.SerializeToElement(sb.ToString());
            }
        }
    }

    private int Dfm(Node node, string path, IncremetalSerializeBag bag)
    {
        bag.Stack.Push(node.Path);
        if (bag.Color.TryGetValue(node.Path, out int col))
        {
            if (col == 1)
            {
                return ZkJsonException.IncrementalCycle;
            }
            bag.Color[node.Path] = 1;
        }
        else
        {
            bag.Color.Add(node.Path, 1);
        }
        if (!node.IsConfirmed)
        {
            Reset(node.Path);
            JsonElement el = JsonSerializer.SerializeToElement(ZkStub.Instance, _jsonSerializerOptions);
            BuildGraph(el, node.Path, bag);
        }
        foreach (var baseNode in node.Bases)
        {
            if (Dfm(baseNode, path, bag) is int ret && ret > 0)
            {
                return ret;
            }
        }
        if(node.Children.Count == 0 && node.Terminals.Count == 0 && node.TmpValues.Count == 0)
        {
            string name1 = CollapseSlashes($"{path}{(node.ValueKind is JsonValueKind.Array ? "[]" : string.Empty)}/{{}}");
            bag.Tree[name1] = JsonSerializer.SerializeToElement<string?>(null);
        }
        else
        {
            foreach (var entry in node.Children)
            {
                if (Dfm(entry.Value, CollapseSlashes($"{path}{(node.ValueKind is JsonValueKind.Array ? "[]" : string.Empty)}/{entry.Key}"), bag) is int ret && ret > 0)
                {
                    return ret;
                }
            }
            foreach (var dict in new Dictionary<string, object>[] { node.Terminals, node.TmpValues })
            {
                foreach (var entry in dict)
                {
                    string name1 = CollapseSlashes($"{path}{(node.ValueKind is JsonValueKind.Array ? "[]" : string.Empty)}/{entry.Key}");
                    if (entry.Value == Node.s_deleted)
                    {
                        bag.Tree.Remove(name1);
                    }
                    else if (entry.Value is JsonElement term)
                    {
                        if (
                            $"{bag.ScriptPrefix}value(" is string prefix
                            && term.ValueKind is JsonValueKind.String
                            && term.GetString() is string s
                            && s.StartsWith(prefix)
                        )
                        {
                            string refPath = s[prefix.Length..(s.Length - 1)];
                            if (bag.Tree.TryGetValue(refPath, out JsonElement el))
                            {
                                term = el;
                            }
                            else
                            {
                                if (!bag.Values.TryGetValue(refPath, out HashSet<string>? set))
                                {
                                    set = [];
                                    bag.Values.Add(refPath, set);
                                }
                                set.Add(name1);
                            }
                        }
                        else if (
                            $"{bag.ScriptPrefix}eval(" is string prefix1
                            && term.ValueKind is JsonValueKind.String
                            && term.GetString() is string s1
                            && s1.StartsWith(prefix1)
                        )
                        {
                            bag.Evals.Add(name1);
                            term = JsonSerializer.SerializeToElement(s1[prefix1.Length..(s1.Length - 1)]);
                        }
                        if (!bag.Tree.TryAdd(name1, term))
                        {
                            bag.Tree[name1] = term;
                        }
                        else
                        {
                            if (bag.Values.TryGetValue(name1, out HashSet<string>? set))
                            {
                                foreach (string name2 in set)
                                {
                                    bag.Tree[name2] = term;
                                }
                                bag.Values.Remove(name1);
                            }
                        }
                    }
                }
            }
        }
        bag.Stack.Pop();
        bag.Color[node.Path] = 2;
        return 0;
    }
    private static Node BuildGraph(JsonElement el, string path, IncremetalSerializeBag bag)
    {
        if (!bag.Nodes.TryGetValue(path, out Node? node))
        {
            node = new Node { Path = $"{path}{(el.ValueKind is JsonValueKind.Array ? "[]" : string.Empty)}", };
            bag.Nodes.Add(node.Path, node);
        }
        node.IsConfirmed = true;

        IEnumerable<object> seq =
            el.ValueKind is JsonValueKind.Object
            ? el.EnumerateObject().Select(v => (object)v)
            : (
                el.ValueKind is JsonValueKind.Array
                ? el.EnumerateArray().Select(v => (object)v)
                : null!
            );
        int pos = 0;
        foreach (object obj in seq)
        {
            if (obj is JsonProperty prop)
            {
                if (pos == 0)
                {
                    node.ValueKind = JsonValueKind.Object;
                }
                if (prop.Name == $"{bag.ScriptPrefix}base")
                {
                    node.BasePaths = (
                        prop.Value.ValueKind is JsonValueKind.String
                        ? new string[] { prop.Value.GetString()! }
                        : (
                            prop.Value.ValueKind is JsonValueKind.Array
                            ? prop.Value.EnumerateArray().Select(v => v.GetString()).ToArray()
                            : throw new JsonException("Base property can only be string or array!") { HResult = ZkJsonException.IncrementalBasePropertyValueKind, }
                        )
                    )!;
                    foreach (string s in node.BasePaths)
                    {
                        string refPath = CollapseSlashes(new Uri(new Uri($"http://localhost{path}"), s).AbsolutePath);
                        if (!bag.Nodes.TryGetValue(refPath, out Node? baseNode))
                        {
                            baseNode = new Node { Path = refPath, };
                            bag.Nodes.Add(refPath, baseNode);
                        }
                        node.Bases.Add(baseNode);
                        baseNode.Inherits.Add(node);
                    }
                }
                else
                {
                    string name;
                    if (prop.Name.StartsWith('-'))
                    {
                        name = prop.Name[1..];
                        node.Terminals.Add(name, Node.s_deleted);
                    }
                    else {
                        name = prop.Name;
                        BuildNode(prop.Value, name);
                    }
                }

            }
            else if (obj is JsonElement item)
            {
                if (pos == 0)
                {
                    node.ValueKind = JsonValueKind.Array;
                }
                string name = pos.ToString();
                BuildNode(item, name);
            }
                ++pos;
        }
        return node;

        void BuildNode(JsonElement item, string name)
        {
            if (item.ValueKind is JsonValueKind.Object || item.ValueKind is JsonValueKind.Array)
            {
                node.Children.Add(name, ZkJsonSerializer.BuildGraph(item, $"{path}/{name}", bag)!);
            }
            else
            {
                JsonElement value;
                if (item.ValueKind is JsonValueKind.String && item.GetString() is string s && s.StartsWith(bag.ScriptPrefix))
                {
                    ZkJsonSerializer.CheckScriptStuff(bag);
                    value = ZkJsonSerializer.ParseScript(s, path, name, bag) ?? item;
                    foreach(var entry in bag.TmpValues)
                    {
                        node.TmpValues.Add(entry.Key, entry.Value);
                    }
                    bag.TmpValues.Clear();
                }
                else
                {
                    value = item;
                }
                node.Terminals.Add(name, value);
            }
        }
    }
    private static void CheckScriptStuff(IncremetalSerializeBag bag)
    {
        if (bag.Script is null)
        {
            bag.Script = new Regex($"^{Regex.Escape(bag.ScriptPrefix)}(?<func>eval|value|path)\\s*\\((?<args>.*?)\\)\\s*$");
            bag.ScriptEval = new Regex($"{Regex.Escape(bag.ScriptPrefix)}(?:value|path)\\s*\\([^()]*\\)");
            bag.EvalTmpValue = new Regex($"{Regex.Escape(bag.ScriptPrefix)}\\d+");
        }
    }
    private static JsonElement? ParseScript(string s, string path, string name, IncremetalSerializeBag bag)
    {
        Match match = bag.Script!.Match(s);
        if (!match.Success)
        {
            throw new ZkJsonException($"Invalid script: {s}")
            {
                HResult = ZkJsonException.IncrementalInvalidScript
            };
        }
        if (match.Groups["func"].Value == "value")
        {
            string args = match.Groups["args"].Value.Trim();
            string refPath = CollapseSlashes(new Uri(new Uri($"http://localhost{path}/{name}"), args).AbsolutePath);
            if (!bag.Nodes.ContainsKey(refPath))
            {
                bag.Nodes.Add(refPath, new Node { Path = refPath, });
            }
            return JsonSerializer.SerializeToElement($"{bag.ScriptPrefix}value({refPath})");
        }
        if (match.Groups["func"].Value == "path")
        {
            string[] args = match.Groups["args"].Value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            string[] refPath = CollapseSlashes($"{path}/{name}").Split('/');
            int from = 0;
            int count = refPath.Length;
            if (args.Length > 0)
            {
                if (!int.TryParse(args[0], out from))
                {
                    throw new ZkJsonException($"{bag.ScriptPrefix}path({match.Groups["args"].Value}): invalid argument(s), must have 0 to 2 integers.")
                    {
                        HResult = ZkJsonException.IncrementalInvalidPathArg
                    };
                }
                if(from < -refPath.Length || from >= refPath.Length)
                {
                    throw new ZkJsonException($"{bag.ScriptPrefix}path({match.Groups["args"].Value}): argument 'from' out of range.")
                    {
                        HResult = ZkJsonException.IncrementalInvalidPathArg
                    };
                }
                if(from <  0)
                {
                    from += refPath.Length;
                }
            }
            if (args.Length > 1)
            {
                if (!int.TryParse(args[1], out count))
                {
                    throw new ZkJsonException($"{bag.ScriptPrefix}path({match.Groups["args"].Value}): invalid argument(s), must have 0 to 2 integers.")
                    {
                        HResult = ZkJsonException.IncrementalInvalidPathArg
                    };
                }
                if(count <= 0 || from + count > refPath.Length)
                {
                    throw new ZkJsonException($"{bag.ScriptPrefix}path({match.Groups["args"].Value}): argument 'count' out of range.")
                    {
                        HResult = ZkJsonException.IncrementalInvalidPathArg
                    };
                }
            }
            return JsonSerializer.SerializeToElement(string.Join('/', refPath[from .. (from + count)]));
        }
        if(match.Groups["func"].Value == "eval")
        {
            StringBuilder sb = new();
            sb.Append(bag.ScriptPrefix).Append("eval(");
            int pos = 0;
            string args = match.Groups["args"].Value;
            for(Match evalMatch = bag.ScriptEval!.Match(args[pos..]); evalMatch.Success; evalMatch = bag.ScriptEval!.Match(args[pos..]))
            {
                sb.Append(args, pos, evalMatch.Groups[0].Index);
                if (evalMatch.Value.StartsWith($"{bag.ScriptPrefix}path("))
                {
                    if(ParseScript(evalMatch.Value, path, name, bag) is JsonElement el)
                    {
                        sb.Append(el.GetString());
                    }
                }
                else
                {
                    string tmpValue = $"{bag.ScriptPrefix}{bag.TmpValueGen}";
                    sb.Append(tmpValue);
                    if (ParseScript(evalMatch.Value, path, name, bag) is JsonElement el)
                    {
                        bag.TmpValues.Add(tmpValue, el);
                    }
                    ++bag.TmpValueGen;
                }
                pos += evalMatch.Groups[0].Index + evalMatch.Length;
            }
            sb.Append(args, pos, args.Length - pos).Append(')');

            return JsonSerializer.SerializeToElement(sb.ToString());
        }
        return null;
    }
}
