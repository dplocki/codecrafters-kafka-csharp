using System.Net;
using System.Net.Sockets;

Console.WriteLine("Server started");

var clusterMetadata = new ClusterMetadata();
var requestFactory = new RequestFactory(clusterMetadata);

using var server = new TcpListener(IPAddress.Any, 9092);
server.Start();

while (true)
{
    var client = await server.AcceptTcpClientAsync();
    _ = Task.Run(async () =>
    {
        Console.WriteLine("Client connected");
        using (client)
        {
            var stream = client.GetStream();
            while (client.Connected) {
                Console.WriteLine("Client request");
                var clientRequestMessage = requestFactory.ParseRequest(stream);
                var module = requestFactory.FindRequestModule(clientRequestMessage);
                await stream.WriteAsync(module.Respond(clientRequestMessage));
            }
        }
    });
}

struct ServerResponseMessage
{
    public int CorrelationId;

    public short Error;

    public readonly byte[] ToMessage()
    {
        var builder = new ResponseBuilder();

        builder.Add4Bytes(CorrelationId);
        builder.Add2Bytes(Error);

        return builder.ToByteArray();
    }
}
