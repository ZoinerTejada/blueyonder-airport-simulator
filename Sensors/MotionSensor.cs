using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Sensors
{
    public class MotionSensor : SensorBase
    {
        public MotionSensor(string deviceId, Action<string> transmitHandler) : base(deviceId, transmitHandler)
        {
        }

        public override void InitSchedule(int reportingIntervalSeconds)
        {
            // schedule for 24 hours
            // rooms starts devoid of motion
            // first flight at 6 am, last flight at 11pm
            // flights arrive every every 90
            // 24x60x60 / 10 = 8640 data points 
            // 30 minutes before flight people arrive, motion occurs 90% of the time 
            // 30 minutes after flight arrives folks are boarded, motion occurs 30% of the time
            // after that reading returns to no motion

            base.InitSchedule(reportingIntervalSeconds);

            int lastDepartureHour = 23; //hour 23 is 11pm
            int minutesBetweenFlights = 90;

            Random random = new Random();

            MotionDataPoint datapoint = new MotionDataPoint()
            {
                activityDetected = false,
                createDate = DateTime.Today.ToUniversalTime(),
                deviceId = this.deviceId
            };

            int numDataPointsPerDay = 24 * 60 * 60 / reportingIntervalSeconds;

            //first flight at 6 am
            int nextDepartureIntervalNumber = 6 * 60 * 60 / reportingIntervalSeconds;

            datapoints = new List<string>(24 * 60 * 60 / reportingIntervalSeconds);

            //i = 0 means midnight
            for (int i = 0; i < numDataPointsPerDay; i++)
            {
                if (IsWithinPreFlightWindow(i, reportingIntervalSeconds, nextDepartureIntervalNumber))
                {
                    datapoint.activityDetected = random.Next(0, 100) >= 10;
                }
                else if (IsWithinPostFlightWindow(i, reportingIntervalSeconds, nextDepartureIntervalNumber))
                {
                    datapoint.activityDetected = random.Next(0, 100) >= 70;
                }
                else
                {
                    datapoint.activityDetected = false;
                }

                datapoints.Add(JsonConvert.SerializeObject(datapoint));

                //prepare the time of the next event
                datapoint.createDate = datapoint.createDate.AddSeconds(reportingIntervalSeconds);

                //set a time for the next departure
                if (HasPlaneDeparted(i, reportingIntervalSeconds, nextDepartureIntervalNumber))
                {
                    nextDepartureIntervalNumber += minutesBetweenFlights * 60 / reportingIntervalSeconds;

                    //e.g., last flight departs at 11 pm
                    if (nextDepartureIntervalNumber * reportingIntervalSeconds >= lastDepartureHour * 60 * 60)
                    {
                        // set the departure to a number in a future day we won't reach
                        nextDepartureIntervalNumber = 30 * 60 * 60 / reportingIntervalSeconds;
                    }
                }
            }

        }

        private bool IsWithinPreFlightWindow(int intervalNumber, int reportingInterval, int departureIntervalNumber)
        {
            //Pre-flight window is 30 minutes before departure
            return (intervalNumber * reportingInterval) >= (departureIntervalNumber * reportingInterval) - (30 * 60) &&
                   (intervalNumber * reportingInterval) < (departureIntervalNumber * reportingInterval);
        }

        private bool IsWithinPostFlightWindow(int intervalNumber, int reportingInterval, int departureIntervalNumber)
        {
            //Post-Flight window lasts from departure to 30 minutes after
            return (intervalNumber * reportingInterval) >= (departureIntervalNumber * reportingInterval) &&
                   (intervalNumber * reportingInterval) < (departureIntervalNumber * reportingInterval) + (30 * 60);
        }

        private bool HasPlaneDeparted(int intervalNumber, int reportingInterval, int departureIntervalNumber)
        {
            return intervalNumber * reportingInterval > departureIntervalNumber * reportingInterval + 30 * 60;
        }

        private class MotionDataPoint
        {
            public bool activityDetected;
            public DateTime createDate;
            public string deviceId;
        }
    }
}
