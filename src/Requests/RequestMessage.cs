public class RequestMessage
{
    public int ApiKey;
    public int ApiVersion;
    public int CorrelationId;
    required public RequestReader RequestReader;
    public string ClientId;
}
