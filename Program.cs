using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using System.IO;

namespace blobtest
{
    struct HistoryPoint
    {
        public DateTimeOffset Timestamp {get;set;}
        public double Value {get;set;}
    }
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = (new ConfigurationBuilder()).AddUserSecrets<Program>().Build();
            var conn = config.GetSection("ConnectionString").Value;

            var edges = new List<Guid>();
            var r = new Random();

            var watch = new Stopwatch();
            watch.Start();
            BlobServiceClient blobServiceClient = new BlobServiceClient(conn);

            // cleanup
            {
                var total = 0;
                foreach (var edge in blobServiceClient.GetBlobContainers())
                {
                    total++;
                    await blobServiceClient.DeleteBlobContainerAsync(edge.Name);
                }

                Console.WriteLine($"Cleanup of {total} edge devices took {watch.Elapsed}");
                watch.Restart();
            }

            var maxEdgeDevices = 1000;

            // creating containers
            {
                for (int i = 0; i < maxEdgeDevices; i++)
                {
                    var g = Guid.NewGuid();
                    edges.Add(g);
                }

                foreach (var edge in edges)
                {
                    var name = edge.ToString();
                    await blobServiceClient.CreateBlobContainerAsync(name);
                }

                Console.WriteLine($"Creation of {maxEdgeDevices} edge devices took {watch.Elapsed}");
                watch.Restart();
            }

            var tasks = new List<Task>();
            
            foreach (var edge in edges)
            {
                Func<Task<bool>> f = async () =>
                {
                    var containerClient = blobServiceClient.GetBlobContainerClient(edge.ToString());
                    // generate x days of history
                    {
                        var maxDays = 365 * 5;
                        var start = DateTime.UtcNow.AddDays(-maxDays).Date;
                        var end = DateTime.UtcNow;
                        var location = "F" + Guid.NewGuid().ToString() + "." + "H" + Guid.NewGuid().ToString();
                        var parameter = "temp";

                        // foreach day, create the blob
                        for (var day = start; day < end; day=day.AddDays(1))
                        {
                            var dayStart = day;
                            var dayEnd = day.AddDays(1);
                            var blobName = $"{location}#{parameter}#{day.ToString("yyyy-MM-dd")}";
                            var blobClient = containerClient.GetBlobClient(blobName);

                            // generate some history 
                            var stream = new MemoryStream();
                            var writer = new BinaryWriter(stream);
                            for (DateTime min = dayStart; min < dayEnd; min = min.AddMinutes(1))
                            {
                                var p = new HistoryPoint() 
                                {
                                    Timestamp = min,
                                    Value = r.NextDouble() * 100
                                };

                                writer.Write(p.Timestamp.ToUnixTimeMilliseconds());
                                writer.Write(p.Value);
                            }

                            stream.Position = 0;

                            // overwrite the blob with data
                            await blobClient.UploadAsync(stream);
                        }
                    }

                    return true;
               };

               tasks.Add(f.Invoke());
            }

            await Task.WhenAll(tasks);
            Console.WriteLine($"Creation of {maxEdgeDevices} edge devices data took {watch.Elapsed}");
        }
    }
}
