using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sensors
{
    public class SensorBase
    {
        protected string deviceId;
        protected Action<string> transmitHandler;
        protected List<string> datapoints;
        protected int reportingIntervalSeconds;

        public int CountOfDataPoints => datapoints.Count;

        protected SensorBase(string deviceId, Action<string> transmitHandler)
        {
            this.deviceId = deviceId;
            this.transmitHandler = transmitHandler;
        }

        public virtual void InitSchedule(int reportingIntervalSeconds)
        {
            this.reportingIntervalSeconds = reportingIntervalSeconds;
        }

        public Task Start()
        {
            return RunAsync();
        }

        private Task RunAsync()
        {
            return Task.Run(() => InternalEmitEvents());
        }

        private void InternalEmitEvents()
        {
            for (int i = 0; i < datapoints.Count; i++)
            {
                transmitHandler.Invoke(datapoints[i]);
            }
        }
    }
}
