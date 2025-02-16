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
        builder.Add32Bits(CorrelationId);
        builder.Add16Bits(Error);

        if (Items != null)
        {
            builder.Add8Bits((byte)(Items.Length + 1));
            foreach (var item in Items)
            {
                builder.Add16Bits(item.ApiKey);
                builder.Add16Bits(item.MinVersion);
                builder.Add16Bits(item.MaxVersion);
                builder.Add8Bits(0);
            }

            builder.Add32Bits(0);
            builder.Add8Bits(0);
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
