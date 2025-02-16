public class RequestMessage
{
    public int ApiKey;
    public int ApiVersion;
    public int CorrelationId;

    required public byte[] RawRequestBody;
}
