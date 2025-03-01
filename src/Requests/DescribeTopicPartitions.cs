internal class DescribeTopicPartitions : IModule
{
    const int UNKNOWN_TOPIC_OR_PARTITION = 3;

    public byte[] Respond(RequestMessage requestMessage)
    {
        var topicArrayLength = requestMessage.RequestReader.Read8Bits();
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
        builder.Add32Bits(CorrelationId);
        builder.Add8Bits(0); // Tag buffer
        builder.Add32Bits(0); // Throttle Time

        builder.Add8Bits((byte)(Topics.Length + 1));

        foreach(var topic in Topics) {
            builder.Add16Bits(Error);
            builder.AddString(topic);

            // topic ID (GUID, 128 bites)
            builder.Add32Bits(0);
            builder.Add32Bits(0);
            builder.Add32Bits(0);
            builder.Add32Bits(0);

            builder.Add8Bits(0); // As internal

            builder.Add8Bits(1); // Partitions Array Length

            builder.Add32Bits(0); // Authorized Operations
            builder.Add8Bits(0); // Tag buffer
        }

        builder.Add8Bits(0xff); // Next cursor (0xff is null)
        builder.Add8Bits(0); // Tag buffer

        return builder.ToByteArray();
    }
}