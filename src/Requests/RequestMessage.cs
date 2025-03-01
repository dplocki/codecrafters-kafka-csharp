public class RequestMessage
{
    public int ApiKey;
    public int ApiVersion;
    public int CorrelationId;
    public required RequestReader RequestReader;
    public required string ClientId;
}
