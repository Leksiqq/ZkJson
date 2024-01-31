namespace Net.Leksi.ZkJson;

public class ZkJsonException: Exception
{
    public const int CannotUpdateArray = 1;
    public const int IncrementalLoop = 2;
    public const int IncrementalNotObject = 3;
    public ZkJsonException() : base() { }
    public ZkJsonException(string? message) : base(message) { }
    public ZkJsonException(string? message, Exception? innerException) : base(message, innerException) { }
}
