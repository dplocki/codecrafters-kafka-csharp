internal class DescribeTopicPartitions : IModule
{
    const short UNKNOWN_TOPIC_OR_PARTITION = 3;
    private readonly IList<DescribeTopic> topics;

    public DescribeTopicPartitions(ClusterMetadata clusterMetadata)
    {
        topics = clusterMetadata.Topics;
    }

    public byte[] Respond(RequestMessage requestMessage)
    {
        var topicArrayLength = requestMessage.RequestReader.Read8Bits() - 1;
        var requestedTopics = new ServerResponseDescribeTopicPartitionsMessageTopic[topicArrayLength];

        for (var i = 0; i < topicArrayLength; i++)
        {
            var topicName = requestMessage.RequestReader.ReadCompactString();
            var loadedTopic = topics.FirstOrDefault(topic => topic!.Name == topicName, null);
            var topicObject = new ServerResponseDescribeTopicPartitionsMessageTopic()
            {
                Name = topicName,
                Error = (short)(loadedTopic == null ? UNKNOWN_TOPIC_OR_PARTITION : 0),
                UUID = loadedTopic?.UUID ?? Guid.Empty,
                Partitions = [],
            };

            requestedTopics[i] = topicObject;

            requestMessage.RequestReader.Read8Bits(); // topic tag buffer
        }

        var result = new ServerResponseDescribeTopicPartitionsMessage
        {
            CorrelationId = requestMessage.CorrelationId,
            Topics = requestedTopics,
        };

        return result.ToMessage();
    }
}

class ServerResponseDescribeTopicPartitionsMessageTopic
{
    public short Error;

    public required string Name;

    public Guid UUID;

    public required IList<DescribeTopicPartition> Partitions;
}

struct ServerResponseDescribeTopicPartitionsMessage
{
    public int CorrelationId;

    public required ServerResponseDescribeTopicPartitionsMessageTopic[] Topics;

    public readonly byte[] ToMessage()
    {
        var builder = new ResponseBuilder();
        builder.Add4Bytes(CorrelationId);
        builder.AddByte(0); // Tag buffer
        builder.Add4Bytes(0); // Throttle Time

        builder.AddByte((byte)(Topics.Length + 1));

        foreach(var topic in Topics) {
            builder.Add2Bytes(topic.Error);
            builder.AddString(topic.Name);

            builder.AddGuid(topic.UUID);

            builder.AddByte(0); // As internal

            builder.AddByte((byte)(topic.Partitions.Count + 1)); // Partitions Array Length

            builder.Add4Bytes(0); // Authorized Operations
            builder.AddByte(0); // Tag buffer
        }

        builder.AddByte(0xff); // Next cursor (0xff is null)
        builder.AddByte(0); // Tag buffer

        return builder.ToByteArray();
    }
}

public class DescribeTopic
{
    public required string Name;

    public Guid UUID;
}

public class DescribeTopicPartition
{
    public int PartitionId;

    public int LeaderId;

    public int[] ReplicaIds;

    public int[] InSyncReplicaIds;

    public int[] OfflineReplicaIds;
}