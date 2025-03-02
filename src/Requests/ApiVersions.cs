public class ApiVersions : IModule
{
    public byte[] Respond(RequestMessage requestMessage)
    {
        var responseApiVersions = new ServerResponseAPIVersionsMessage
        {
            CorrelationId = requestMessage.CorrelationId,
            Error = 0,
        };

        if (requestMessage.ApiVersion >= 0 && requestMessage.ApiVersion <= 4)
        {
            responseApiVersions.Items = [
                new APIVersionItem()
                {
                    ApiKey = 18,
                    MinVersion = 4,
                    MaxVersion = 4,
                },
                new APIVersionItem()
                {
                    ApiKey = 75,
                    MinVersion = 0,
                    MaxVersion = 0,
                }
            ];
        }
        else
        {
            responseApiVersions.Error = 35;
            responseApiVersions.Items = [];
        }

        return responseApiVersions.ToMessage();
    }
}

struct ServerResponseAPIVersionsMessage
{
    public int CorrelationId;

    public short Error;

    public APIVersionItem[] Items;

    public readonly byte[] ToMessage()
    {
        var builder = new ResponseBuilder();
        builder.Add4Bytes(CorrelationId);
        builder.Add2Bytes(Error);

        if (Items != null)
        {
            builder.AddByte((byte)(Items.Length + 1));
            foreach (var item in Items)
            {
                builder.Add2Bytes(item.ApiKey);
                builder.Add2Bytes(item.MinVersion);
                builder.Add2Bytes(item.MaxVersion);
                builder.AddByte(0);
            }

            builder.Add4Bytes(0);
            builder.AddByte(0);
        }

        return builder.ToByteArray();
    }
}

struct APIVersionItem
{
    public short ApiKey;
    public short MinVersion;
    public short MaxVersion;
}
