using System.Buffers.Binary;

public class RequestFactory
{
    private Dictionary<int, IModule> modules;

    public RequestFactory()
    {
        modules = new Dictionary<int, IModule>
        {
            { 18, new ApiVersions() }
        };
    }

    public async Task<RequestMessage> ParseRequest(Stream stream)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer);
        var messageSize = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer.AsSpan());

        var buffer = new byte[messageSize];
        await stream.ReadExactlyAsync(buffer);

        return new RequestMessage()
        {
            ApiKey = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(0, 2)),
            ApiVersion = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(2, 2)),
            CorrelationId = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(4, 4)),
            RawRequestBody = buffer.AsSpan(8).ToArray()
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
