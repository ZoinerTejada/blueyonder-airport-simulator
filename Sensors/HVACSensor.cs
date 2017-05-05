using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Sensors
{
    public class HVACSensor : SensorBase
    {
        public HVACSensor(string deviceId, Action<string> transmitHandler) : base(deviceId, transmitHandler)
        {
        }

        public override void InitSchedule(int reportingIntervalSeconds)
        {
            // schedule for 24 hours
            // rooms starts at ambient temp
            // first flight at 6 am, last flight at 11pm
            // flights arrive every every 90
            // 24x60x60 / 10 = 8640 data points 
            // cooling is activated for a 5 minute window every hour 

            base.InitSchedule(reportingIntervalSeconds);

            HVACDataPoint datapoint = new HVACDataPoint()
            {
                state = ActivationState.noChange,
                createDate = DateTime.Today.ToUniversalTime(),
                deviceId = this.deviceId
            };

            int runDurationMinutes = 5;
            int numDataPointsPerDay = 24 * 60 * 60 / reportingIntervalSeconds;
            int lastActivationInterval = 0;

            datapoints = new List<string>(24 * 60 * 60 / reportingIntervalSeconds);

            //i = 0 means midnight
            for (int i = 0; i < numDataPointsPerDay; i++)
            {
                if (IsAtScheduledActivationMilestone(i, reportingIntervalSeconds))
                {
                    datapoint.state = datapoint.state = ActivationState.coolingActivated;
                    datapoints.Add(JsonConvert.SerializeObject(datapoint));
                    lastActivationInterval = i;
                }
                else if (IsAtScheduledDeactivationMilestone(i, reportingIntervalSeconds, lastActivationInterval, runDurationMinutes))
                {
                    datapoint.state = datapoint.state = ActivationState.coolingDeactivated;
                    datapoints.Add(JsonConvert.SerializeObject(datapoint));
                }

                //prepare the time of the next event
                datapoint.createDate = datapoint.createDate.AddSeconds(reportingIntervalSeconds);
            }
        }

        private bool IsAtScheduledActivationMilestone(int intervalNumber, int reportingInterval)
        {
            return intervalNumber * reportingInterval % (60 * 60) == 0;
        }

        private bool IsAtScheduledDeactivationMilestone(int intervalNumber, int reportingInterval, int lastActivationInterval, int runDurationMinutes)
        {
            return intervalNumber * reportingInterval == lastActivationInterval * reportingInterval + runDurationMinutes * 60;
        }

        private enum ActivationState
        {
            noChange = 0,
            heatActivated = 1,
            coolingActivated = 2,
            heatDeactivated = 3,
            coolingDeactivated = 4
        }

        private class HVACDataPoint
        {
            public ActivationState state;
            public DateTime createDate;
            public string deviceId;
        }
    }
}
