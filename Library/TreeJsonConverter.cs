using System.Text.Json;
using System.Text.Json.Serialization;

namespace Net.Leksi.ZkJson;

internal class TreeJsonConverter : JsonConverter<Dictionary<string, JsonElement>>
{
    public override Dictionary<string, JsonElement>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, Dictionary<string, JsonElement> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        List<Tuple<string, JsonValueKind>> stack = [];
        foreach(string key in value.Keys.OrderBy(k => k))
        {
            //Console.WriteLine(key);
            string[] parts = key.Split('/', StringSplitOptions.RemoveEmptyEntries);
            while(stack.Count >= parts.Length || (stack.Count > 0 && !parts.Zip(stack, (f, s) => f == s.Item1).All(v => v)))
            {
                Tuple<string, JsonValueKind> tup = stack.Last();
                stack.RemoveAt(stack.Count - 1);
                if(tup.Item2 is JsonValueKind.Object)
                {
                    writer.WriteEndObject();
                }
                else
                {
                    writer.WriteEndArray();
                }
            }
            for(int i = stack.Count; i < parts.Length - 1; ++i)
            {
                if (parts[i].EndsWith("[]"))
                {
                    string name = parts[i].Substring(0, parts[i].Length - 2);
                    writer.WritePropertyName(name);
                    stack.Add(new Tuple<string, JsonValueKind>(parts[i], JsonValueKind.Array));
                    writer.WriteStartArray();
                }
                else
                {
                    writer.WritePropertyName(parts[i]);
                    stack.Add(new Tuple<string, JsonValueKind>(parts[i], JsonValueKind.Object));
                    writer.WriteStartObject();
                }
            }
            if (stack.Count == 0 || stack.Last().Item2 is JsonValueKind.Object)
            {
                writer.WritePropertyName(parts.Last());
            }
            JsonSerializer.Serialize(writer, value[key]);
        }
        while (stack.Count > 0)
        {
            Tuple<string, JsonValueKind> tup = stack.Last();
            stack.RemoveAt(stack.Count - 1);
            if (tup.Item2 is JsonValueKind.Object)
            {
                writer.WriteEndObject();
            }
            else
            {
                writer.WriteEndArray();
            }
        }
        writer.WriteEndObject();
    }
}
