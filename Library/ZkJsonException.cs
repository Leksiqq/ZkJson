namespace Net.Leksi.ZkJson;

public class ZkJsonException: Exception
{
    public const int CannotUpdateArray = 1;
    public const int IncrementalCycle = 2;
    public const int IncrementalBasePropertyValueKind = 3;
    public const int IncrementalOutOfTree = 4;
    public ZkJsonException() : base() { }
    public ZkJsonException(string? message) : base(message) { }
    public ZkJsonException(string? message, Exception? innerException) : base(message, innerException) { }
}
