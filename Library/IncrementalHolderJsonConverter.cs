using System.Text.Json;
using System.Text.Json.Serialization;

namespace Net.Leksi.ZkJson;

internal class IncrementalHolderJsonConverter : JsonConverter<IncrementalHolder>
{
    public override IncrementalHolder? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, IncrementalHolder value, JsonSerializerOptions options)
    {
        if(value._value is Dictionary<string, IncrementalHolder> dict)
        {
            writer.WriteStartObject();
            foreach(var it in dict)
            {
                writer.WritePropertyName(it.Key);
                JsonSerializer.Serialize(writer, it.Value, options);
            }
            writer.WriteEndObject();
        }
        else if(value._value is List<IncrementalHolder> list)
        {
            writer.WriteStartArray();
            foreach (var it in list)
            {
                JsonSerializer.Serialize(writer, it, options);
            }
            writer.WriteEndArray();
        }
        else if(value._value is JsonElement el)
        {
            JsonSerializer.Serialize(writer, JsonSerializer.Deserialize<object>(el), options);
        }
    }
}