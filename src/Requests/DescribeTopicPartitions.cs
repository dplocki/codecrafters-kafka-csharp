using System.Text;

internal class DescribeTopicPartitions : IModule
{
    const string CLUSTER_METADATA_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    const short UNKNOWN_TOPIC_OR_PARTITION = 3;

    public byte[] Respond(RequestMessage requestMessage)
    {
        var topics = LoadTopics();
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

    private IList<DescribeTopic> LoadTopics()
    {
        var result = new List<DescribeTopic>();
        // open the CLUSTER_METADATA_PATH file as stream
        var stream = File.OpenRead(CLUSTER_METADATA_PATH);
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

            for(var indexRecord = 0; indexRecord < recordsCountInt; indexRecord++) {
                reader.ReadBytes(
                    1 + 1 + 1 + 1 // Record Length + Attributes + Timestamp Delta + Offset Delta
                );

                reader.ReadByte(); // key length
                // reader.ReadBytes(keyLength); // Key

                var valueLength = reader.ReadByte();
                if (valueLength >= 4) {
                    reader.ReadByte(); //  Frame Version
                    var valueType = reader.ReadByte();
                    if (valueType == 2) // Topic Record
                    {
                        reader.ReadBytes(1); // Version
                        var nameLength = reader.ReadByte();
                        var topicName = Encoding.UTF8.GetString(reader.ReadBytes(nameLength));
                        var guid = new Guid(reader.ReadBytes(16));
                        reader.ReadByte(); // Tagged Fields Count
                        reader.ReadBytes(valueLength - 1 - 1 - 1 - 1 - nameLength - 16 - 1); // Length - Frame Version - Value Type - Version - Name Length - Name - UUID - Tagged Fields Count

                        result.Add(new DescribeTopic
                        {
                            Name = topicName,
                            UUID = guid,
                        });
                    }
                    else
                    {
                        reader.ReadBytes(valueLength - 2); // Length - (Frame Version + Value Type)
                    }
                } else {
                    reader.ReadBytes(valueLength);
                }

                reader.ReadBytes(1); //  Headers Array Count
            }
        }

        return result;
    }
}

class ServerResponseDescribeTopicPartitionsMessageTopic
{
    public short Error;

    public required string Name;

    public Guid UUID;
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

            builder.AddByte(1); // Partitions Array Length

            builder.Add4Bytes(0); // Authorized Operations
            builder.AddByte(0); // Tag buffer
        }

        builder.AddByte(0xff); // Next cursor (0xff is null)
        builder.AddByte(0); // Tag buffer

        return builder.ToByteArray();
    }
}

class DescribeTopic
{
    public required string Name;

    public Guid UUID;
}