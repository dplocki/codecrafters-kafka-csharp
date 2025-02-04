using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

async static Task<ClientRequestMessage> ParseClientRequestMessage(Stream stream)
{
    var buffer = new byte[12];
    var messageSize = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, 4));

    await stream.ReadAsync(buffer);
    return new ClientRequestMessage()
    {
        RequestApiKey = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(4, 2)),
        RequestApiVersion = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(6, 2)),
        CorrelationId = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(8, 4))
    };
}

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

var server = new TcpListener(IPAddress.Any, 9092);
server.Start();

using var client = await server.AcceptTcpClientAsync();
Console.WriteLine("Client connected!");

var stream = client.GetStream();
var clientRequestMessage = await ParseClientRequestMessage(stream);

var response = new ServerResponseMessage()
{
    CorrelationId = clientRequestMessage.CorrelationId,
    Error = clientRequestMessage.RequestApiVersion != 4
        ? 35
        : null,
};

await stream.WriteAsync(response.ToMessage());
await stream.FlushAsync();

struct ClientRequestMessage
{
    public int RequestApiKey;
    public int RequestApiVersion;
    public int CorrelationId;
}

struct ServerResponseMessage
{
    public int CorrelationId;

    public short? Error;

    public readonly byte[] ToMessage()
    {
        var messageSize = Error.HasValue ? 10 : 8;
        var responseHeaderBuffer = new byte[messageSize];

        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(0, 4), messageSize);
        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(4, 4), CorrelationId);

        if (Error.HasValue) {
            BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(8, 2), Error.Value);
        }

        return responseHeaderBuffer;
    }
}
