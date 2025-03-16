using System.Buffers.Binary;

class ResponseBuilder : IDisposable
{
    const byte SizeOfSize = 4;
    readonly MemoryStream stream = new();

    public ResponseBuilder()
    {
        stream.Write(new byte[SizeOfSize], 0, SizeOfSize);
    }

    public ResponseBuilder AddByte(byte value)
    {
        stream.WriteByte(value);

        return this;
    }

    public ResponseBuilder Add2Bytes(short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        stream.Write(buffer);

        return this;
    }

    public ResponseBuilder Add4Bytes(int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        stream.Write(buffer);

        return this;
    }

    public ResponseBuilder AddGuid(Guid value)
    {
        Span<byte> buffer = stackalloc byte[16];
        value.TryWriteBytes(buffer);
        stream.Write(buffer);

        return this;
    }


    public ResponseBuilder AddString(string value)
    {
        AddByte((byte)(value.Length + 1));

        foreach(var letter in value)
        {
            AddByte((byte)letter);
        }

        return this;
    }
    public void Dispose()
    {
        stream.Dispose();
    }

    public byte[] ToByteArray()
    {
        var result = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(0, SizeOfSize), result.Length - SizeOfSize);

        return result;
    }
}

