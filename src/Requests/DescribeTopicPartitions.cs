using System.Text;

internal class DescribeTopicPartitions : IModule
{
    const int UNKNOWN_TOPIC_OR_PARTITION = 3;

    public byte[] Respond(RequestMessage requestMessage)
    {
        var topicLength = requestMessage.RawRequestBody[1];

        var result = new ServerResponseDescribeTopicPartitionsMessage
        {
            CorrelationId = requestMessage.CorrelationId,
            Error = UNKNOWN_TOPIC_OR_PARTITION,
            Topics = [
                Encoding.ASCII.GetString(requestMessage.RawRequestBody.AsSpan(2, topicLength - 1)),
            ],
        };

        return result.ToMessage();
    }
}

struct ServerResponseDescribeTopicPartitionsMessage
{
    public int CorrelationId;

    public short Error;

    public APIVersionItem[] Items;

    public string[] Topics;

    public readonly byte[] ToMessage()
    {
        var builder = new ResponseBuilder();
        builder.Add32Bits(CorrelationId);
        builder.Add8Bits(0); // Tag buffer
        builder.Add32Bits(0); // Throttle Time

        builder.Add8Bits((byte)Topics.Length);

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

            builder.Add8Bits(0); // Tag buffer
        }

        return builder.ToByteArray();
    }
}