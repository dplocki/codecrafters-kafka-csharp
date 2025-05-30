using System.Text;

public class ClusterMetadata
{
    const string CLUSTER_METADATA_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    public IDictionary<string, DescribeTopic> Topics { get; private set; }
    public IEnumerable<DescribeTopicPartition> Partitions { get; private set; }

    public ClusterMetadata()
    {
        var (topics, partitions) = LoadTopics(CLUSTER_METADATA_PATH);

        Topics = topics.ToDictionary(topic => topic.Name, topic => topic);
        Partitions = partitions;
    }

    private (IList<DescribeTopic>, IList<DescribeTopicPartition>) LoadTopics(string clusterMetadataLoaderPath)
    {
        var topics = new List<DescribeTopic>();
        var partitions = new List<DescribeTopicPartition>();
        var stream = File.OpenRead(clusterMetadataLoaderPath);
        var reader = new BinaryReader(stream);

        while(reader.BaseStream.Position < reader.BaseStream.Length)
        {
            reader.ReadBytes(8); // Base Offset
            reader.ReadBytes(4); // Batch Length
            reader.ReadBytes(4); // Partition Leader Epoch
            reader.ReadBytes(1); // Magic Byte
            reader.ReadBytes(4); // CRC
            reader.ReadBytes(2); // Attributes
            reader.ReadBytes(4); // Last Offset Delta
            reader.ReadBytes(8); // First Timestamp
            reader.ReadBytes(8); // Max Timestamp
            reader.ReadBytes(8); // Producer ID
            reader.ReadBytes(2); // Producer Epoch
            reader.ReadBytes(4); // Base Sequence

            var recordsCountBytes = reader.ReadBytes(4);
            var recordsCountInt = (recordsCountBytes[0] << 24) | (recordsCountBytes[1] << 16) | (recordsCountBytes[2] << 8) | recordsCountBytes[3];

            for(var indexRecord = 0; indexRecord < recordsCountInt; indexRecord++)
            {
                DecodeVarInt(reader); // Record Length
                reader.ReadBytes(
                    1 + 1 + 1 // Attributes + Timestamp Delta + Offset Delta
                );

                var keyLength = DecodeVarInt(reader);
                if (keyLength != -1)
                {
                    reader.ReadBytes(keyLength); // Key
                }

                var valueLength = DecodeVarInt(reader);
                if (valueLength >= 4)
                {
                    reader.ReadByte(); //  Frame Version
                    var valueType = reader.ReadByte();

                    if (valueType == 2) // Topic Record
                    {
                        reader.ReadByte(); // Version
                        var nameLength = reader.ReadByte() - 1;
                        var topicName = Encoding.UTF8.GetString(reader.ReadBytes(nameLength));
                        var guid = new Guid(reader.ReadBytes(16));
                        reader.ReadByte(); // Tagged Fields Count

                        topics.Add(new DescribeTopic
                        {
                            Name = topicName,
                            UUID = guid,
                        });

                        reader.ReadBytes(valueLength // Length
                             - 1 - 1 - 1             // - Frame Version - Value Type - Version
                             - 1 - nameLength        // - Name Length - Name
                             - 16 - 1);              // - UUID - Tagged Fields Count Frame
                    }
                    else if (valueType == 3) // Partition Record
                    {
                        reader.ReadByte(); // Version
                        reader.ReadBytes(4); // Partition ID

                        var guid = new Guid(reader.ReadBytes(16));

                        var replicaArraylength = reader.ReadByte() - 1;
                        for (var indexReplica = 0; indexReplica < replicaArraylength; indexReplica++)
                        {
                            reader.ReadBytes(4); // Replica
                        }

                        var removeArrayLength = reader.ReadByte() - 1;
                        for (var indexReplica = 0; indexReplica < removeArrayLength; indexReplica++)
                        {
                            reader.ReadBytes(4); // Replica
                        }

                        var addedArrayLength = reader.ReadByte() - 1;
                        for (var indexReplica = 0; indexReplica < addedArrayLength; indexReplica++)
                        {
                            reader.ReadBytes(1); // Replica
                        }

                        reader.ReadBytes(4); // Leader
                        reader.ReadBytes(4); // Leader Epoch
                        reader.ReadBytes(4); // Partition Epoch
                        reader.ReadByte(); // Length of Directories array

                        var directoriesArrayLength = reader.ReadByte() - 1;
                        for (var indexReplica = 0; indexReplica < directoriesArrayLength; indexReplica++)
                        {
                            reader.ReadBytes(16); // Replica
                        }

                        reader.ReadByte(); // Tagged Fields Count

                        partitions.Add(new DescribeTopicPartition
                        {
                            UUID = guid,
                            InSyncReplicaIds = [],
                            ReplicaIds = [],
                            OfflineReplicaIds = [],
                        });
                    }
                    else
                    {
                        reader.ReadBytes(valueLength - 2); // Length - (Frame Version + Value Type)
                    }
                }
                else
                {
                    reader.ReadBytes(valueLength);
                }

                reader.ReadByte(); //  Headers Array Count
            }
        }

        return (topics, partitions);
    }

    private static int DecodeVarInt(BinaryReader reader)
    {
        int result = 0;
        int shift = 0;
        byte currentByte;

        do
        {
            currentByte = reader.ReadByte();
            result |= (currentByte & 0x7F) << shift;
            shift += 7;
        }
        while ((currentByte & 0x80) != 0);

        return (result >> 1) ^ -(result & 1);
    }
}
