public class RequestFactory
{
    private readonly Dictionary<int, IModule> modules;

    public RequestFactory()
    {
        modules = new Dictionary<int, IModule>
        {
            { 18, new ApiVersions() },
            { 75, new DescribeTopicPartitions() }
        };
    }

    public RequestMessage ParseRequest(Stream stream)
    {
        var requestReader = new RequestReader(stream);

        return new RequestMessage()
        {
            ApiKey = requestReader.Read16Bites(),
            ApiVersion = requestReader.Read16Bites(),
            CorrelationId = requestReader.Read32Bites(),
            RequestReader = requestReader,
        };
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
