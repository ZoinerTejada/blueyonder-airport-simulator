using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.Threading;

namespace AirportSimulatorConsole
{
    class Program
    {
        static string eventHubName = "blueyonderairports";
        static string connectionString = "Endpoint=sb://blueyonderairports-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=2c3Q0+TBHQrN6fJy5zh9sRpfG2tNFa3JdQqo87XZ0L8=";
        static int numDevices = 41525;
        static int delayIntervalMS = 10000;
        static int numMessagesPerInterval = 9368;

        static System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
        static Random rng = new Random();
        static List<string> devices;
        static List<EventHubClient> senders;
        static List<EventData> messages;
        static int numSenders;
        static long estimatedMessageSize;
        static int estimatedNumMessagesPerBatch;
        static int messageCounter = 0;

        static void Main(string[] args)
        {
            Console.WriteLine("Press Ctrl-C to stop the sender process");
            
            InitDevices();

            InitClients();
            
            PrepareMessages();

            Console.WriteLine("Press Enter to start sending now");
            Console.ReadLine();

            SendingRandomEvents();
        }

        static void InitDevices()
        {
            Console.WriteLine("Initializing devices...");
            devices = new List<string>(numDevices);
            for (int i = 0; i < numDevices; i++)
            {
                devices.Add(Guid.NewGuid().ToString());
            }
        }

        static void InitClients()
        {
            //Approximate the size of a message
            var message = new
            {
                temp = rng.Next(60, 78),
                createDate = DateTime.UtcNow,
                deviceId = devices[0]
            };

            var jsonMessage = JsonConvert.SerializeObject(message);
            EventData eventData = new EventData(Encoding.UTF8.GetBytes(jsonMessage));
            estimatedMessageSize = eventData.SerializedSizeInBytes;

            numSenders = (int)Math.Ceiling((double) numMessagesPerInterval * estimatedMessageSize / (256*1024)) ;
            estimatedNumMessagesPerBatch = (int) Math.Ceiling( (double) (256 * 1024) / estimatedMessageSize );

            senders = new List<EventHubClient>(numSenders);
            EventHubClient client;

            Console.WriteLine("Creating {0} batch senders handling {1} messages each...", numSenders, estimatedNumMessagesPerBatch);

            for (int i = 0; i < numSenders; i++)
            {
                client = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);
                senders.Add(client);
            }

            Console.WriteLine("Senders are ready.");
        }

        static void PrepareMessages()
        {
            messages = new List<EventData>(numMessagesPerInterval);
            DateTime createDate;
            EventData eventData;

            //Pre-create messages
            Console.WriteLine("Preparing messages...");
            createDate = DateTime.UtcNow; //simulate all events in batch having same timestamp

            for (int i = 0; i < numMessagesPerInterval; i++)
            {
                var message = new
                {
                    temp = rng.Next(60, 78),
                    createDate = DateTime.UtcNow,
                    deviceId = devices[i % numSenders]
                };

                var jsonMessage = JsonConvert.SerializeObject(message);
                eventData = new EventData(Encoding.UTF8.GetBytes(jsonMessage));

                messages.Add(eventData);
                messageCounter++;
            }

            Console.WriteLine("Messages ready to send.");
        }

        static void SendingRandomEvents()
        {
            while (true)
            {
                Console.WriteLine("Sending prepared messages...");
                stopwatch.Restart();

                try
                {
                    int numComplete = 0;


                    for (int i = 0; i < senders.Count; i++)
                    {
                        try
                        {
                            int numMessagesToSend = i == senders.Count - 1 ? (int)(messages.Count - i * estimatedNumMessagesPerBatch) : estimatedNumMessagesPerBatch;
                            Console.WriteLine("{0}: Sending batch {1} of size {2} messages...", DateTime.Now, i, numMessagesToSend);
                            Task t = senders[i].SendBatchAsync(messages.GetRange((int)i * estimatedNumMessagesPerBatch, numMessagesToSend));
                            t.ContinueWith((a) =>
                            {
                                Interlocked.Increment(ref numComplete);
                                Console.WriteLine("{0}: Completed batch", DateTime.Now);
                            });
                        }
                        catch (Exception e)
                        {
                            LogError(e.Message);
                        }
                    }

                    while (numComplete < senders.Count)
                    {
                        Thread.Sleep(50);
                    }

                    Console.WriteLine("All {0:N} messages sent in {1:N2} seconds ({2:N} messages/second, {3:N} B/s).", messageCounter, stopwatch.Elapsed.TotalSeconds, messageCounter / stopwatch.Elapsed.TotalSeconds, (messageCounter * estimatedMessageSize) / stopwatch.Elapsed.TotalSeconds);

                    stopwatch.Stop();

                }
                catch (Exception ex)
                {
                    LogError(ex.Message);
                }

                Console.WriteLine("Waiting for interval...");
                Thread.Sleep(delayIntervalMS);

            }
           
        }

        private static void LogError(String message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("{0} > Exception {1}", DateTime.Now, message);
            Console.ResetColor();
        }
    }
}
