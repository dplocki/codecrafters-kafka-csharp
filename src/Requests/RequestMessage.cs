public class RequestMessage
{
    public int ApiKey;
    public int ApiVersion;
    public int CorrelationId;
    public required RequestByteStreamReader RequestReader;
    public required string ClientId;
}
