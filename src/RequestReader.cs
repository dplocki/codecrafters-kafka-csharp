using System.Buffers.Binary;

public class RequestReader
{
    private readonly byte[] messageBuffer;
    private int index = 0;

    public RequestReader(Stream stream)
    {
        var sizeBuffer = new byte[4];
        stream.ReadExactly(sizeBuffer);
        var messageSize = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer.AsSpan());

        messageBuffer = new byte[messageSize];
        stream.ReadExactly(messageBuffer);
    }

    public byte Read8its() {
        return messageBuffer[index++];
    }

    public short Read16bites() {
        var value = BinaryPrimitives.ReadInt16BigEndian(messageBuffer.AsSpan(index, 2));
        index += 2;
        return value;
    }

    public int Read32bites() {
        var value = BinaryPrimitives.ReadInt32BigEndian(messageBuffer.AsSpan(index, 4));
        index += 4;
        return value;
    }
}