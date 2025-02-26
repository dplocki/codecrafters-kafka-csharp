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
            Topic = Encoding.ASCII.GetString(requestMessage.RawRequestBody.AsSpan(2, topicLength - 1)),
        };

        return result.ToMessage();
    }
}

struct ServerResponseDescribeTopicPartitionsMessage
{
    public int CorrelationId;

    public short Error;

    public APIVersionItem[] Items;

    public string Topic;

    public readonly byte[] ToMessage()
    {
        var builder = new ResponseBuilder();
        builder.Add32Bits(CorrelationId);
        builder.Add16Bits(Error);
        builder.AddString(Topic);
        builder.Add32Bits(1);
        builder.Add32Bits(0);

        return builder.ToByteArray();
    }
}