using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sensors;
using Microsoft.ServiceBus.Messaging;
using System.Diagnostics;

namespace SimpleSensorConsole
{
    class Program
    {
        static bool useEventHub = true;

        static void Main(string[] args)
        {
            InterviewUser();

            if (useEventHub)
            {
                EventHubLoadSimulator simulator = new EventHubLoadSimulator();
                simulator.Init();
                simulator.SimulateTemperatureEvents();
                simulator.SimulateMotionEvents();
                simulator.SimulateHVACEvents();
            }
            else
            {
                IoTHubLoadSimulator simulator = new IoTHubLoadSimulator();
                simulator.Init();
                simulator.SimulateTemperatureEvents().Wait();
                simulator.SimulateMotionEvents().Wait();
                simulator.SimulateHVACEvents().Wait();
            }

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        private static void InterviewUser()
        {
            Console.WriteLine("To what service should events be sent (type 1 or 2 and press enter)?");
            Console.WriteLine("1. Event Hubs");
            Console.WriteLine("2. IoT Hub");

            var key = Console.ReadKey();
            Console.WriteLine("");

            if (key.KeyChar.Equals('1'))
            {
                useEventHub = true;
                Console.WriteLine("Using Event Hubs");
            }
            else
            {
                useEventHub = false;
                Console.WriteLine("Using IoT Hub");
            }
            
        }
    }
}
