using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

async static Task<ClientRequestMessage> ParseClientRequestMessage(Stream stream)
{
    var buffer = new byte[12];
    var messageSize = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, 4));

    await stream.ReadExactlyAsync(buffer);
    return new ClientRequestMessage()
    {
        ApiKey = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(4, 2)),
        ApiVersion = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(6, 2)),
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

if (clientRequestMessage.ApiKey == 18)
{
    var responseApiVersions = new ServerResponseAPIVersionsMessage()
    {
        CorrelationId = clientRequestMessage.CorrelationId,
        Error = 0,
        Items = [
            new APIVersionItem()
            {
                ApiKey = 18,
                MinVersion = 4,
                MaxVersion = 4,
            }
        ],
    };

    await stream.WriteAsync(responseApiVersions.ToMessage());
    await stream.FlushAsync();
}

var response = new ServerResponseMessage()
{
    CorrelationId = clientRequestMessage.CorrelationId,
    Error = (clientRequestMessage.ApiVersion != 4) ? (short)35 : (short)0,
};

await stream.WriteAsync(response.ToMessage());
await stream.FlushAsync();

struct ClientRequestMessage
{
    public int ApiKey;
    public int ApiVersion;
    public int CorrelationId;
}

struct ServerResponseMessage
{
    public int CorrelationId;

    public short Error;

    public readonly byte[] ToMessage()
    {
        var messageSize = 10;
        var responseHeaderBuffer = new byte[messageSize];

        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(0, 4), messageSize);
        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(4, 4), CorrelationId);
        BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(8, 2), Error);

        return responseHeaderBuffer;
    }
}

struct APIVersionItem
{
    public short ApiKey;
    public short MinVersion;
    public int MaxVersion;
}

struct ServerResponseAPIVersionsMessage
{
    public int CorrelationId;

    public short Error;

    public APIVersionItem[] Items;

    public readonly byte[] ToMessage()
    {
        var messageSize = 4 + 4 + 2 + 2 + Items.Length * (2 + 2 + 4);
        var responseHeaderBuffer = new byte[messageSize];

        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(0, 4), messageSize);
        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(4, 4), CorrelationId);
        BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(8, 2), Error);
        BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(10, 2), (short)Items.Length);

        var index = 12;
        foreach(var item in Items)
        {
            BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(index, 2), item.ApiKey);
            index += 2;
            BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(index, 2), item.MinVersion);
            index += 2;
            BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(index, 4), item.MaxVersion);
            index += 4;
        }

        BinaryPrimitives.WriteInt32BigEndian(responseHeaderBuffer.AsSpan(index, 4), 0);
        index += 4;
        BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(index, 1), 1);
        index += 1;
        BinaryPrimitives.WriteInt16BigEndian(responseHeaderBuffer.AsSpan(index, 1), 0);

        return responseHeaderBuffer;
    }
}