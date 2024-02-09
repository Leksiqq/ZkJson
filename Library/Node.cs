using System.Text.Json;

namespace Net.Leksi.ZkJson;

internal class Node
{
    internal static readonly object s_deleted = new();
    internal string Path { get; set; } = null!;
    internal HashSet<Node> Bases { get; private init; } = [];
    internal Dictionary<string, Node> Children { get; private init; } = [];
    internal Dictionary<string, object> Terminals { get; private init; } = [];
    internal Dictionary<string, object> TmpValues { get; private init; } = [];
    internal HashSet<Node> Inherits { get; private init; } = [];
    internal bool IsConfirmed { get; set; } = false;
    internal JsonValueKind ValueKind { get; set; }
    internal List<string> Ordered { get; private init; } = [];
    internal Node? Parent { get; set; } = null;
    internal string[]? BasePaths { get; set; } = null;
}
