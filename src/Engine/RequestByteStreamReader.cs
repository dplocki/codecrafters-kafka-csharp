using System.Buffers.Binary;
using System.Text;

public class RequestByteStreamReader
{
    private readonly byte[] messageBuffer;
    private int index = 0;

    public RequestByteStreamReader(Stream stream)
    {
        var sizeBuffer = new byte[4];
        stream.ReadExactly(sizeBuffer);
        var messageSize = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer.AsSpan());

        messageBuffer = new byte[messageSize];
        stream.ReadExactly(messageBuffer);
    }

    public byte Read8Bits() {
        return messageBuffer[index++];
    }

    public short Read16Bites() {
        var value = BinaryPrimitives.ReadInt16BigEndian(messageBuffer.AsSpan(index, 2));
        index += 2;
        return value;
    }

    public int Read32Bites() {
        var value = BinaryPrimitives.ReadInt32BigEndian(messageBuffer.AsSpan(index, 4));
        index += 4;
        return value;
    }

    public string ReadString() {
        var length = Read16Bites();
        var value = messageBuffer.AsSpan(index, length);

        index += length;

        return Encoding.ASCII.GetString(value);
    }

    public string ReadCompactString() {
        var length = Read8Bits() - 1;
        var value = messageBuffer.AsSpan(index, length);

        index += length;

        return Encoding.ASCII.GetString(value);
    }
}