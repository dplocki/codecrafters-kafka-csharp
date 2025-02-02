using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

async static Task<ClientRequestMessage> ParseClientRequestMessage(Stream stream)
{
    var buffer = new byte[12];

    await stream.ReadAsync(buffer);
    return new ClientRequestMessage()
    {
        MessageSize = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, 4)),
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
    MessageSize = 0,
    CorrelationId = clientRequestMessage.CorrelationId,
    Error = clientRequestMessage.RequestApiVersion != 4
        ? 35
        : null,
};

await stream.WriteAsync(response.ToMessage());
await stream.FlushAsync();

struct ClientRequestMessage
{
    public int MessageSize;
    public int RequestApiKey;
    public int RequestApiVersion;
    public int CorrelationId;
}

struct ServerResponseMessage
{
    public int MessageSize;
    public int CorrelationId;

    public short? Error;

    public readonly byte[] ToMessage()
    {
        var responseHeaderBuffer = new byte[Error.HasValue ? 10 : 8];
        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(0, 4), MessageSize);
        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(4, 4), CorrelationId);

        if (Error.HasValue) {
            BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(8, 2), Error.Value);
        }

        return responseHeaderBuffer;
    }
}
