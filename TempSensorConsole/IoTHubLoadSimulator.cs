using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Client.Exceptions;
using Sensors;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleSensorConsole
{
    public class IoTHubLoadSimulator
    {
        string iotHubSenderConnectionString;
        string iotHubManagerConnectionString;

        RegistryManager registryManager;

        bool sendAsBatch; //Set this to false in config to send events one at a time

        long maxBatchSizeInBytes = 256 * 1024;
        long bufferedSizeInBytes;
        List<Microsoft.Azure.Devices.Client.Message> sendBuffer = new List<Microsoft.Azure.Devices.Client.Message>(100);

        Stopwatch stopWatch = new System.Diagnostics.Stopwatch();
        int numEventsSent;

        string deviceId;
        string deviceKey;
        DeviceClient deviceClient;

        public void Init()
        {
            try
            {
                iotHubSenderConnectionString = System.Configuration.ConfigurationManager.AppSettings["IoTHubSenderConnectionsString"];
                iotHubManagerConnectionString = System.Configuration.ConfigurationManager.AppSettings["IoTHubManagerConnectionsString"];
                sendAsBatch = bool.Parse(System.Configuration.ConfigurationManager.AppSettings["SendEventsAsBatch"]);

                registryManager = RegistryManager.CreateFromConnectionString(iotHubManagerConnectionString);

            }
            catch (Exception ex)
            {
                LogError(ex.Message);
                throw;
            }

        }

        public async Task SimulateTemperatureEvents()
        {
            deviceId = "1";

            RegisterDeviceAsync().Wait();
            bool deviceActivated = await ActivateDeviceAsync();

            if (deviceActivated)
            {
                InitDeviceClient();

                stopWatch.Restart();
                numEventsSent = 0;
                LogStatus("Sending Temperature Events...");

                SensorBase sensor = new TemperatureSensor(deviceId, TransmitEvent);
                sensor.InitSchedule(10);
                Console.WriteLine("Generated {0:###,###,###} Events", sensor.CountOfDataPoints);
                sensor.Start().Wait();
                FlushIoTHubBuffer();
                stopWatch.Stop();

                Console.WriteLine("Completed transmission in {0} seconds. Sent {1:###,###,###} events.", 
                    stopWatch.Elapsed.TotalSeconds, numEventsSent);
            }
            else
            {
                LogError("Device Not Activated.");
            } 
        }

        public async Task SimulateMotionEvents()
        {
            deviceId = "2";

            RegisterDeviceAsync().Wait();
            bool deviceActivated = await ActivateDeviceAsync();

            if (deviceActivated)
            {
                InitDeviceClient();

                stopWatch.Restart();
                numEventsSent = 0;
                LogStatus("Sending Motion Events...");

                SensorBase sensor = new MotionSensor(deviceId, TransmitEvent);
                sensor.InitSchedule(10);
                Console.WriteLine("Generated {0:###,###,###} Events", sensor.CountOfDataPoints);
                sensor.Start().Wait();
                FlushIoTHubBuffer();
                stopWatch.Stop();

                Console.WriteLine("Completed transmission in {0} seconds. Sent {1:###,###,###} events.", stopWatch.Elapsed.TotalSeconds, numEventsSent);
            }
            else
            {
                LogError("Device Not Activated.");
            }
        }

        public async Task SimulateHVACEvents()
        {
            deviceId = "3";

            RegisterDeviceAsync().Wait();
            bool deviceActivated = await ActivateDeviceAsync();

            if (deviceActivated)
            {
                InitDeviceClient();

                stopWatch.Restart();
                numEventsSent = 0;
                LogStatus("Sending HVAC Events...");

                SensorBase sensor = new HVACSensor(deviceId, TransmitEvent);
                sensor.InitSchedule(10);
                Console.WriteLine("Generated {0:###,###,###} Events", sensor.CountOfDataPoints);
                sensor.Start().Wait();
                FlushIoTHubBuffer();
                stopWatch.Stop();

                Console.WriteLine("Completed transmission in {0} seconds. Sent {1:###,###,###} events.", stopWatch.Elapsed.TotalSeconds, numEventsSent);
            }
            else
            {
                LogError("Device Not Activated.");
            }
        }

        void InitDeviceClient()
        {
            var builder = Microsoft.Azure.Devices.IotHubConnectionStringBuilder.Create(iotHubSenderConnectionString);
            string iotHubName = builder.HostName;

            deviceClient = DeviceClient.Create(iotHubName, 
                new DeviceAuthenticationWithRegistrySymmetricKey(deviceId, deviceKey));
        }

        async Task RegisterDeviceAsync()
        {
            Device device = new Device(deviceId);
            device.Status = DeviceStatus.Disabled;

            try
            {
                device = await registryManager.AddDeviceAsync(device);
            }
            catch (Microsoft.Azure.Devices.Common.Exceptions.DeviceAlreadyExistsException)
            {
                //Device already exists, get the registered device
                device = await registryManager.GetDeviceAsync(deviceId);

                //Ensure the device is disabled until Activated later
                device.Status = DeviceStatus.Disabled;

                //Update IoT Hubs with the device status change
                await registryManager.UpdateDeviceAsync(device);
            }

            deviceKey = device.Authentication.SymmetricKey.PrimaryKey;
        }

        async Task<bool> ActivateDeviceAsync()
        {
            bool success = false;
            Device device;

            try
            {
                //Fetch the device
                device = await registryManager.GetDeviceAsync(deviceId);

                //Verify the device keys match
                if (deviceKey == device.Authentication.SymmetricKey.PrimaryKey)
                {
                    //Enable the device
                    device.Status = DeviceStatus.Enabled;

                    //Update IoT Hubs
                    await registryManager.UpdateDeviceAsync(device);

                    success = true;
                }
            }
            catch (Exception)
            {
                success = false;
            }

            return success;
        }

        void TransmitEvent(string datapoint)
        {
            Microsoft.Azure.Devices.Client.Message message;
            try
            {
                message = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes(datapoint));

                if (sendAsBatch)
                {
                    SendToIoTHubAsBatch(message);
                }
                else
                {
                    SendToIoTHubDirect(message);
                }

                //NOTE: Fastest execution time happens without console output.
                //LogStatus(datapoint);
            }
            catch (Exception ex)
            {
                LogError(ex.Message);
            }
        }

        void SendToIoTHubAsBatch(Microsoft.Azure.Devices.Client.Message message)
        {

            long currMessageSizeInBytes = message.GetBytes().LongLength ;

            if (bufferedSizeInBytes + currMessageSizeInBytes >= maxBatchSizeInBytes)
            {
                FlushIoTHubBuffer();
            }

            sendBuffer.Add(message);
            bufferedSizeInBytes += currMessageSizeInBytes;
        }

        void FlushIoTHubBuffer()
        {
            if (sendBuffer.Count > 0)
            {
                deviceClient.SendEventBatchAsync(sendBuffer);

                numEventsSent += sendBuffer.Count;
                sendBuffer.Clear();
                bufferedSizeInBytes = 0;
            }
        }

        void SendToIoTHubDirect(Microsoft.Azure.Devices.Client.Message message)
        {
            deviceClient.SendEventAsync(message);
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
