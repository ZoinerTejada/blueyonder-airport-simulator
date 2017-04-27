using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sensors;
using Microsoft.ServiceBus.Messaging;

namespace SimpleSensorConsole
{

    public class EventHubLoadSimulator
    {
        string eventHubsConnectionString;

        EventHubClient eventHubClient;

        bool sendAsBatch; //Set this to false in config to send events one at a time

        long maxBatchSizeInBytes = 256 * 1024;
        long bufferedSizeInBytes;
        List<EventData> sendBuffer = new List<EventData>(100);

        Stopwatch stopWatch = new System.Diagnostics.Stopwatch();
        int numEventsSent;

        public void Init()
        {
            try
            {
                eventHubsConnectionString = System.Configuration.ConfigurationManager.AppSettings["EventHubsSenderConnectionString"];
                eventHubClient = EventHubClient.CreateFromConnectionString(eventHubsConnectionString);
                sendAsBatch = bool.Parse(System.Configuration.ConfigurationManager.AppSettings["SendEventsAsBatch"]);
            }
            catch (Exception ex)
            {
                LogError(ex.Message);
                throw;
            }

        }

        public void SimulateTemperatureEvents()
        {
            stopWatch.Restart();
            numEventsSent = 0;
            LogStatus("Sending Temperature Events...");
            SensorBase sensor = new TemperatureSensor("1", TransmitEvent);
            sensor.InitSchedule(10);
            Console.WriteLine("Generated {0:###,###,###} Events", sensor.CountOfDataPoints);
            sensor.Start().Wait();
            FlushEventHubBuffer();
            stopWatch.Stop();
            Console.WriteLine("Completed transmission in {0} seconds. Sent {1:###,###,###} events.", 
                stopWatch.Elapsed.TotalSeconds, numEventsSent);            
        }

        public void SimulateMotionEvents()
        {
            stopWatch.Restart();
            numEventsSent = 0;
            LogStatus("Sending Motion Events...");
            SensorBase sensor = new MotionSensor("2", TransmitEvent);
            sensor.InitSchedule(10);
            Console.WriteLine("Generated {0:###,###,###} Events", sensor.CountOfDataPoints);
            sensor.Start().Wait();
            FlushEventHubBuffer();
            stopWatch.Stop();
            Console.WriteLine("Completed transmission in {0} seconds. Sent {1:###,###,###} events.", stopWatch.Elapsed.TotalSeconds, numEventsSent);
        }

        public void SimulateHVACEvents()
        {
            stopWatch.Restart();
            numEventsSent = 0;
            LogStatus("Sending HVAC Events...");
            SensorBase sensor = new HVACSensor("3", TransmitEvent);
            sensor.InitSchedule(10);
            Console.WriteLine("Generated {0:###,###,###} Events", sensor.CountOfDataPoints);
            sensor.Start().Wait();
            FlushEventHubBuffer();
            stopWatch.Stop();
            Console.WriteLine("Completed transmission in {0} seconds. Sent {1:###,###,###} events.", stopWatch.Elapsed.TotalSeconds, numEventsSent);
        }

        void TransmitEvent(string datapoint)
        {
            EventData eventData;
            try
            {
                eventData = new EventData(Encoding.UTF8.GetBytes(datapoint));

                if (sendAsBatch)
                {
                    SendToEventHubAsBatch(eventData);
                }
                else
                {
                    SendToEventHubDirect(eventData);
                }

                //NOTE: Fastest execution time happens without console output.
                //LogStatus(datapoint);
            }
            catch (Exception ex)
            {
                LogError(ex.Message);
            }
        }

        void SendToEventHubAsBatch(EventData eventData)
        {
            long currEventSizeInBytes = eventData.SerializedSizeInBytes;

            if (bufferedSizeInBytes + currEventSizeInBytes >= maxBatchSizeInBytes)
            {
                FlushEventHubBuffer();
            }

            sendBuffer.Add(eventData);
            bufferedSizeInBytes += currEventSizeInBytes;
        }

        void FlushEventHubBuffer()
        {
            if (sendBuffer.Count > 0)
            {
                eventHubClient.SendBatch(sendBuffer);

                numEventsSent += sendBuffer.Count;
                sendBuffer.Clear();
                bufferedSizeInBytes = 0;
            }
        }

        void SendToEventHubDirect(EventData eventData)
        {
            eventHubClient.Send(eventData);
        }

        void LogError(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("{0} > Exception {1}", DateTime.Now, message);
            Console.ResetColor();
        }

        void LogStatus(string message)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(message);
            Console.ResetColor();
        }
    }
}
