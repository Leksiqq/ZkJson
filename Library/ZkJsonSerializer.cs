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
    private BinaryWriter? _binWriter = null;
    private BinaryReader? _binReader = null;
    private readonly JsonSerializerOptions _jsonSerializerOptions = new();

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
    public JsonElement IncrementalSerialize(string basePropertyName)
    {
        JsonElement source = JsonSerializer.SerializeToElement(ZkStub.Instance, _jsonSerializerOptions);
        if (source.ValueKind is not JsonValueKind.Object && source.ValueKind is not JsonValueKind.Array)
        {
            return source;
        }
        Dictionary<string, Node> nodes = [];
        string saveRoot = Root;
        Node root = BuildGraph(source, nodes, Root, basePropertyName, null);
        Tree tree = new(); ;
        ResolveReferences(root, tree, nodes, basePropertyName);
        Root = saveRoot;
        RemovePrefix(tree, Root);
        return JsonSerializer.SerializeToElement(tree);
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
    private static void RemovePrefix(Tree tree, string prefix)
    {
        int len = tree._ordered.Count;
        for(int i = 0; i < len; ++i)
        {
            string prefix1 = CollapseSlashes($"{prefix}/");
            if (!tree._ordered[i].StartsWith(prefix1))
            {
                throw new ZkJsonException($"Reference out of tree: {tree._ordered[i]}") { HResult = ZkJsonException.IncrementalOutOfTree, };
            }
            string newPath = $"/{tree._ordered[i][prefix1.Length..]}";
            tree._ordered.Add(newPath);
            JsonElement el = tree._dict[tree._ordered[i]];
            tree._dict.Remove(tree._ordered[i]);
            tree._dict.Add(newPath, el);
        }
        tree._ordered.RemoveRange(0, len);
    }
    private void ResolveReferences(Node node, Tree tree, Dictionary<string, Node> nodes, string basePropertyName)
    {
        Stack<string> stack = [];
        Dictionary<string, int> color = [];
        int result = Dfm(node, color, stack, tree, node.Path, nodes, basePropertyName);
        if (result > 0)
        {
            ZkJsonException ex;
            if (result == ZkJsonException.IncrementalCycle)
            {
                ex = new ZkJsonException("A cycle was detected!") { HResult = result };
                List<string> cycle = [];
                while (stack.Count > 0)
                {
                    cycle.Add(stack.Pop());
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
    }

    private int Dfm(Node node, Dictionary<string, int> color, Stack<string> stack, Tree tree, string path, Dictionary<string, Node> nodes, string basePropertyName)
    {
        stack.Push(node.Path);
        if (color.TryGetValue(node.Path, out int col))
        {
            if (col == 1)
            {
                return ZkJsonException.IncrementalCycle;
            }
            color[node.Path] = 1;
        }
        else
        {
            color.Add(node.Path, 1);
        }
        if (!node.IsConfirmed)
        {
            Reset(node.Path);
            JsonElement el = JsonSerializer.SerializeToElement(ZkStub.Instance, _jsonSerializerOptions);
            BuildGraph(el, nodes, node.Path, basePropertyName, null);
        }
        foreach (var baseNode in node.Bases)
        {
            if (Dfm(baseNode, color, stack, tree, path, nodes, basePropertyName) is int ret && ret > 0)
            {
                return ret;
            }
        }
        foreach (string name in node.Ordered)
        {
            if (node.Children.TryGetValue(name, out Node? child))
            {
                if (Dfm(child, color, stack, tree, CollapseSlashes($"{path}{(node.ValueKind is JsonValueKind.Array ? "[]" : string.Empty)}/{name}"), nodes, basePropertyName) is int ret && ret > 0)
                {
                    return ret;
                }
            }
            else if (node.Terminals.TryGetValue(name, out object? obj))
            {
                string name1 = CollapseSlashes($"{path}{(node.ValueKind is JsonValueKind.Array ? "[]" : string.Empty)}/{name}");
                if (obj == Node.s_deleted)
                {
                    tree._dict.Remove(name1);
                    tree._ordered.Remove(name1);
                }
                else if (obj is JsonElement term)
                {
                    if (tree._dict.TryGetValue(name1, out var _))
                    {
                        tree._dict[name1] = term;
                    }
                    else
                    {
                        tree._dict.Add(name1, term);
                        tree._ordered.Add(name1);
                    }
                }
            }
        }
        stack.Pop();
        color[node.Path] = 2;
        return 0;
    }

    private static Node BuildGraph(JsonElement el, Dictionary<string, Node> nodes, string path, string basePropertyName, Node? parent)
    {
        if (!nodes.TryGetValue(path, out Node? node))
        {
            node = new Node { Path = $"{path}{(el.ValueKind is JsonValueKind.Array ? "[]" : string.Empty)}", };
            nodes.Add(node.Path, node);
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
                if (prop.Name == basePropertyName)
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
#if DEBUG && VERBOSE
                    Console.WriteLine();
                    Console.WriteLine($"path: {path}");
#endif
                    foreach (string s in node.BasePaths)
                    {
                        string refPath = CollapseSlashes(new Uri(new Uri($"http://localhost{path}"), s).AbsolutePath);
#if DEBUG && VERBOSE
                        Console.WriteLine($"    {basePropertyName}: {s} -> {refPath}");
#endif
                        if (!nodes.TryGetValue(refPath, out Node? baseNode))
                        {
                            baseNode = new Node { Path = refPath, };
                            nodes.Add(refPath, baseNode);
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
                    else if (prop.Value.ValueKind is JsonValueKind.Object || prop.Value.ValueKind is JsonValueKind.Array)
                    {
                        name = prop.Name;
                        node.Children.Add(name, BuildGraph(prop.Value, nodes, CollapseSlashes($"{path}/{prop.Name}"), basePropertyName, node)!);
                    }
                    else
                    {
                        name = prop.Name;
                        node.Terminals.Add(name, prop.Value);
                    }
                    node.Ordered!.Add(name);
                }

            }
            else if (obj is JsonElement item)
            {
                if (pos == 0)
                {
                    node.ValueKind = JsonValueKind.Array;
                }
                string name = pos.ToString();
                if (item.ValueKind is JsonValueKind.Object || item.ValueKind is JsonValueKind.Array)
                {
                    node.Children.Add(name, BuildGraph(item, nodes, $"{path}/{pos}", basePropertyName, node)!);
                }
                else
                {
                    node.Terminals.Add(name, item);
                }
                node.Ordered!.Add(name);
            }
            ++pos;
        }
        return node;
    }
}
