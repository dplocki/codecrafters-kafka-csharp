public class NoModule : IModule
{
    public byte[] Respond(RequestMessage requestMessage)
    {
        var response = new ServerResponseMessage()
        {
            CorrelationId = requestMessage.CorrelationId,
            Error = (requestMessage.ApiVersion != 4) ? (short)35 : (short)0,
        };

        return response.ToMessage();
    }
}
