using System.Text.Json;
using System.Text.Json.Serialization;

namespace Net.Leksi.ZkJson;

[JsonConverter(typeof(TreeJsonConverter))]
internal class Tree
{
    internal Dictionary<string, JsonElement> _dict = [];
    internal List<string> _ordered = [];
}
