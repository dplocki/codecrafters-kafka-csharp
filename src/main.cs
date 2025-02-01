using System.Net;
using System.Net.Sockets;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

var server = new TcpListener(IPAddress.Any, 9092);
server.Start();

int correlationId = 7;

using var client = await server.AcceptTcpClientAsync(); // Accept client connection
Console.WriteLine("Client connected!");

var stream = client.GetStream();

var messageSizeBytes = BitConverter.GetBytes(0);
Array.Reverse(messageSizeBytes);
await stream.WriteAsync(messageSizeBytes);

var messageContentBytes = BitConverter.GetBytes(correlationId);
Array.Reverse(messageContentBytes);
await stream.WriteAsync(messageContentBytes);
await stream.FlushAsync();
