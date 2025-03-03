using System.Text;

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

    private IEnumerable<DescribeTopic> LoadTopics()
    {
        var result = new List<DescribeTopic>();
        // open the CLUSTER_METADATA_PATH file as stream
        var stream = File.OpenRead(CLUSTER_METADATA_PATH);
        var reader = new BinaryReader(stream);

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

        var recordsCountInt = BitConverter.ToInt32(reader.ReadBytes(4), 0);

        for(var indexRecord = 0; indexRecord < recordsCountInt; indexRecord++) {
            reader.ReadBytes(
                1 + 1 + 1  // Record Length + Attributes + Timestamp Delta
                + 1 + 1 + 1 // Offset Delta + Key Length + Key
            );

            var valueLength = reader.ReadByte();

            reader.ReadBytes(1); //  Frame Version
            var valueType = reader.ReadByte();
            if (valueType == 2) // Topic Record
            {
                reader.ReadBytes(1); // Version
                var nameLength = reader.ReadByte();
                var topicName = Encoding.UTF8.GetString(reader.ReadBytes(nameLength));
                var guid = new Guid(reader.ReadBytes(16));
                reader.ReadByte(); // Tagged Fields Count

                result.Add(new DescribeTopic
                {
                    Name = topicName,
                    UUID = guid,
                });
            }
            else
            {
                reader.ReadBytes(valueLength - 2); // Length - Frame Version + Value Type
            }

            reader.ReadBytes(1); //  Headers Array Count
        }

        return result;
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

    public Guid UUID;
}