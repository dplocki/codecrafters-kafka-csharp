internal class DescribeTopicPartitions : IModule
{
    const string CLUSTER_METADATA_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    const int UNKNOWN_TOPIC_OR_PARTITION = 3;

    public byte[] Respond(RequestMessage requestMessage)
    {
        var topics = LoadTopics();

        var topicArrayLength = requestMessage.RequestReader.Read8Bits() - 1;
        var requestedTopics = new string[topicArrayLength];

        for (var i = 0; i < topicArrayLength; i++)
        {
            requestedTopics[i] = requestMessage.RequestReader.ReadCompactString();
            requestMessage.RequestReader.Read8Bits(); // topic tag buffer
        }

        var result = new ServerResponseDescribeTopicPartitionsMessage
        {
            CorrelationId = requestMessage.CorrelationId,
            Error = UNKNOWN_TOPIC_OR_PARTITION,
            Topics = requestedTopics,
        };

        return result.ToMessage();
    }

    private DescribeTopic[] LoadTopics()
    {
        // return File.ReadAllBytes(CLUSTER_METADATA_PATH);
        return [];
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

struct DescribeTopic
{
    public string Name;

    public Guid GenericUriParserOptions;
}