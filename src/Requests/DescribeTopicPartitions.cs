internal class DescribeTopicPartitions : IModule
{
    const int UNKNOWN_TOPIC_OR_PARTITION = 3;

    public byte[] Respond(RequestMessage requestMessage)
    {
        var topicArrayLength = requestMessage.RequestReader.Read8Bits() - 1;
        var topics = new string[topicArrayLength];

        for(var i = 0; i < topicArrayLength; i++) {
            topics[i] = requestMessage.RequestReader.ReadCompactString();
            requestMessage.RequestReader.Read8Bits(); // topic tag buffer
        }

        var result = new ServerResponseDescribeTopicPartitionsMessage
        {
            CorrelationId = requestMessage.CorrelationId,
            Error = UNKNOWN_TOPIC_OR_PARTITION,
            Topics = topics,
        };

        return result.ToMessage();
    }
}

struct ServerResponseDescribeTopicPartitionsMessage
{
    public int CorrelationId;

    public short Error;

    public string[] Topics;

    public readonly byte[] ToMessage()
    {
        var builder = new ResponseBuilder();
        builder.Add4Bytes(CorrelationId);
        builder.AddByte(0); // Tag buffer
        builder.Add4Bytes(0); // Throttle Time

        builder.AddByte((byte)(Topics.Length + 1));

        foreach(var topic in Topics) {
            builder.Add2Bytes(Error);
            builder.AddString(topic);

            // topic ID (GUID, 128 bites)
            builder.Add4Bytes(0);
            builder.Add4Bytes(0);
            builder.Add4Bytes(0);
            builder.Add4Bytes(0);

            builder.AddByte(0); // As internal

            builder.AddByte(1); // Partitions Array Length

            builder.Add4Bytes(0); // Authorized Operations
            builder.AddByte(0); // Tag buffer
        }

        builder.AddByte(0xff); // Next cursor (0xff is null)
        builder.AddByte(0); // Tag buffer

        return builder.ToByteArray();
    }
}