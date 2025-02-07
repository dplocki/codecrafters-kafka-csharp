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
    var responseApiVersions = new ServerResponseAPIVersionsMessage
    {
        CorrelationId = clientRequestMessage.CorrelationId,
        Error = 0,
    };

    if (clientRequestMessage.ApiVersion <= 4) {
        responseApiVersions.Items = [
            new APIVersionItem()
            {
                ApiKey = 18,
                MinVersion = 4,
                MaxVersion = 4,
            }
        ];
    } else {
        responseApiVersions.Error = 35;
    }

    await stream.WriteAsync(responseApiVersions.ToMessage());
    await stream.FlushAsync();
    return;
}

var response = new ServerResponseMessage()
{
    CorrelationId = clientRequestMessage.CorrelationId,
    Error = (clientRequestMessage.ApiVersion != 4) ? (short)35 : (short)0,
};

await stream.WriteAsync(response.ToMessage());
await stream.FlushAsync();

class ResponseBuilder : IDisposable
{
    readonly MemoryStream stream = new();
    int index = 0;

    public ResponseBuilder()
    {
        stream.Write([0, 0, 0, 0], 0, 4);
    }

    public ResponseBuilder Add8Bits(byte value)
    {
        stream.WriteByte(value);

        return this;
    }

    public ResponseBuilder Add16Bits(short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        stream.Write(buffer);

        return this;
    }

    public ResponseBuilder Add32Bits(int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);

        return this;
    }


    public void Dispose()
    {
        stream.Dispose();
    }

    public byte[] ToByteArray()
    {
        var result = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(0, 4), result.Length - 4);

        return result;
    }
}

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
        var builder = new ResponseBuilder();

        builder.Add32Bits(CorrelationId);
        builder.Add16Bits(Error);

        return builder.ToByteArray();
    }
}

struct APIVersionItem
{
    public short ApiKey;
    public short MinVersion;
    public short MaxVersion;
}

struct ServerResponseAPIVersionsMessage
{
    public int CorrelationId;

    public short Error;

    public APIVersionItem[] Items;

    public readonly byte[] ToMessage()
    {
        var builder = new ResponseBuilder();
        builder.Add32Bits(CorrelationId);
        builder.Add16Bits(Error);

        if (Items != null)
        {
            builder.Add8Bits((byte)(Items.Length + 1));
            foreach(var item in Items)
            {
                builder.Add16Bits(item.ApiKey);
                builder.Add16Bits(item.MinVersion);
                builder.Add16Bits(item.MaxVersion);
            }

            builder.Add8Bits(0);
            builder.Add32Bits(0);
            builder.Add8Bits(0);
        }

        return builder.ToByteArray();
    }
}