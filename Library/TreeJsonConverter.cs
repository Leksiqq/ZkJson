using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Linq;

namespace Net.Leksi.ZkJson;

internal class TreeJsonConverter : JsonConverter<Tree>
{
    public override Tree? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, Tree value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        Stack<Tuple<string, JsonValueKind>> stack = [];
        foreach(string key in value._ordered)
        {
            string[] parts = key.Split('/');
            while(stack.Count >= parts.Length || parts[stack.Count - 1] != stack.Peek().Item1)
            {
                var tup = stack.Pop();
                if(tup.Item2 is JsonValueKind.Object)
                {
                    writer.WriteEndObject();
                }
                else
                {
                    writer.WriteEndArray();
                }
            }
            for(int i = depth; i < parts.Length - 1; ++i)
            {
                if (parts[i].EndsWith("[]"))
                {
                    string name = parts[i].Substring(0, parts[i].Length - 2);
                    writer.WritePropertyName(name);
                    stack.Push(new Tuple<string, JsonValueKind>(parts[i], JsonValueKind.Array));
                    writer.WriteStartArray();
                }
                else
                {
                    writer.WritePropertyName(parts[i]);
                    stack.Push(new Tuple<string, JsonValueKind>(parts[i], JsonValueKind.Object));
                    writer.WriteStartObject();
                }
            }
        }
        writer.WriteEndObject();
    }
}
