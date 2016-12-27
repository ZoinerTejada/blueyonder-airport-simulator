using System;
using System.Collections.Generic;
using System.Configuration;
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
        static int numDevices = 41525;
        static int delayIntervalMS = 10000;
        static int numMessagesPerInterval = 9368;

        static System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
        static Random random = new Random();
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
                temp = random.Next(60, 78),
                createDate = DateTime.UtcNow,
                deviceId = devices[0]
            };

            var jsonMessage = JsonConvert.SerializeObject(message);
            EventData eventData = new EventData(Encoding.UTF8.GetBytes(jsonMessage));
            estimatedMessageSize = eventData.SerializedSizeInBytes;

            numSenders = (int)Math.Ceiling((double)numMessagesPerInterval * estimatedMessageSize / (256 * 1024));
            estimatedNumMessagesPerBatch = (int)Math.Ceiling((double)(256 * 1024) / estimatedMessageSize);

            senders = new List<EventHubClient>(numSenders);
            Console.WriteLine($"Creating {numSenders} batch senders handling {estimatedNumMessagesPerBatch} messages each...");

            var eventHubConnectionString = ConfigurationManager.AppSettings["eventHubConnectionString"];
            var eventHubName = ConfigurationManager.AppSettings["eventHubName"];

            for (int i = 0; i < numSenders; i++)
            {
                EventHubClient client = EventHubClient.CreateFromConnectionString(eventHubConnectionString, eventHubName);
                senders.Add(client);
            }

            Console.WriteLine("Senders are ready.");
        }

        static void PrepareMessages()
        {
            messages = new List<EventData>(numMessagesPerInterval);
            DateTime batchCreationDate;
            EventData eventData;

            //Pre-create messages
            Console.WriteLine("Preparing messages...");
            batchCreationDate = DateTime.UtcNow; //simulate all events in batch having same timestamp

            for (int i = 0; i < numMessagesPerInterval; i++)
            {
                var message = new
                {
                    temp = random.Next(60, 78),
                    createDate = batchCreationDate,
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
                            int numMessagesToSend = i == senders.Count - 1 ? messages.Count - i * estimatedNumMessagesPerBatch : estimatedNumMessagesPerBatch;
                            Console.WriteLine($"{DateTime.Now}: Sending batch {i} of size {numMessagesToSend} messages...");
                            Task task = senders[i].SendBatchAsync(messages.GetRange(i * estimatedNumMessagesPerBatch, numMessagesToSend));
                            task.ContinueWith((a) =>
                            {
                                Interlocked.Increment(ref numComplete);
                                Console.WriteLine($"{DateTime.Now}: Completed batch");
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
            Console.WriteLine($"{DateTime.Now} > Exception {message}");
            Console.ResetColor();
        }
    }
}
