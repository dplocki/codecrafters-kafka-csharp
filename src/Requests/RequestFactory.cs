using System.Buffers.Binary;

public class RequestFactory
{
    public async Task<BaseRequest> ParseRequest(Stream stream)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer);
        var messageSize = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer.AsSpan());

        var buffer = new byte[messageSize];
        await stream.ReadExactlyAsync(buffer);

        return new BaseRequest()
        {
            ApiKey = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(0, 2)),
            ApiVersion = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(2, 2)),
            CorrelationId = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(4, 4))
        };
    }
}