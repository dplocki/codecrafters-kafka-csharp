internal class DescribeTopicPartitions : IModule
{
    const short UNKNOWN_TOPIC_OR_PARTITION = 3;

    private readonly ClusterMetadata clusterMetadata;

    public DescribeTopicPartitions(ClusterMetadata clusterMetadata)
    {
        this.clusterMetadata = clusterMetadata;
    }


    public byte[] Respond(RequestMessage requestMessage)
    {
        var topicArrayLength = requestMessage.RequestReader.Read8Bits() - 1;
        var requestedTopics = new ServerResponseDescribeTopicPartitionsMessageTopic[topicArrayLength];


        for (var i = 0; i < topicArrayLength; i++)
        {
            var topicName = requestMessage.RequestReader.ReadCompactString();

            clusterMetadata.Topics.TryGetValue(topicName, out var topic);
            var UUID = topic?.UUID ?? Guid.Empty;
            var partitions = clusterMetadata.Partitions.Where(p => p.UUID == UUID).ToArray();

            var topicObject = new ServerResponseDescribeTopicPartitionsMessageTopic()
            {

                Name = topicName,
                Error = (short)(topic == null ? UNKNOWN_TOPIC_OR_PARTITION : 0),
                UUID = UUID,
                Partitions = partitions,
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

            foreach(var partition in topic.Partitions) {
                builder.Add2Bytes(0); // Error
                builder.Add4Bytes(partition.PartitionId);
                builder.AddGuid(partition.UUID);
                builder.Add4Bytes(partition.LeaderId);

                builder.AddByte((byte)(partition.ReplicaIds.Length + 1)); // Replica Array Length
                foreach (var replicaId in partition.ReplicaIds) {
                    builder.Add4Bytes(replicaId);
                }

                builder.AddByte((byte)(partition.InSyncReplicaIds.Length + 1)); // In Sync Replica Array Length
                foreach (var inSyncReplicaId in partition.InSyncReplicaIds) {
                    builder.Add4Bytes(inSyncReplicaId);
                }

                builder.AddByte((byte)(partition.OfflineReplicaIds.Length + 1)); // Offline Replica Array Length
                foreach (var offlineReplicaId in partition.OfflineReplicaIds) {
                    builder.Add4Bytes(offlineReplicaId);
                }
            }

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
    public Guid UUID;

    public int LeaderId;

    public required int[] ReplicaIds;

    public required int[] InSyncReplicaIds;

    public required int[] OfflineReplicaIds;
}