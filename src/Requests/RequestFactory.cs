public class RequestFactory
{
    private readonly Dictionary<int, IModule> modules;

    public RequestFactory(ClusterMetadata clusterMetadata)
    {
        modules = new Dictionary<int, IModule>
        {
            { 18, new ApiVersions() },
            { 75, new DescribeTopicPartitions(clusterMetadata) }
        };
    }

    public RequestMessage ParseRequest(Stream stream)
    {
        var requestReader = new RequestByteStreamReader(stream);
        var result = new RequestMessage()
        {
            ApiKey = requestReader.Read16Bites(),
            ApiVersion = requestReader.Read16Bites(),
            CorrelationId = requestReader.Read32Bites(),
            ClientId = requestReader.ReadString(),

            RequestReader = requestReader,
        };

        requestReader.Read8Bits(); // Tag buffer

        return result;
    }

    public IModule FindRequestModule(RequestMessage request)
    {
        if (modules.TryGetValue(request.ApiKey, out IModule? value))
        {
            return value;
        }

        return new NoModule();
    }
}
