using System.Text.Json;
using System.Text.RegularExpressions;

namespace Net.Leksi.ZkJson;

internal class IncremetalSerializeBag
{
    internal string ScriptPrefix = null!;
    internal Regex? Script;
    internal Regex? ScriptEval;
    internal Regex? EvalTmpValue;
    internal Dictionary<string, Node> Nodes = [];
    internal Dictionary<string, JsonElement> Tree = [];
    internal Stack<string> Stack = [];
    internal Dictionary<string, int> Color = [];
    internal Dictionary<string, HashSet<string>> Values = [];
    internal int TmpValueGen = 0;
    internal Dictionary<string, JsonElement> TmpValues = [];
    internal List<string> Evals = [];
}
