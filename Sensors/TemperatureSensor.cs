using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sensors
{
    public class TemperatureSensor: SensorBase
    {
        public TemperatureSensor(string deviceId, Action<string> transmitHandler) : base(deviceId, transmitHandler)
        {
        }

        public override void InitSchedule(int reportingIntervalSeconds)
        {
            // schedule for 24 hours
            // rooms start at ambient temperature (65 F) of airport
            // first flight at 6 am, last flight at 11pm
            // flights arrive every every 90
            // 24x60x60 / 10 = 8640 data points 
            // 30 minutes before flight people arrive, temp starts to rise due to warm from bodies (rise 5 degrees)
            // 30 minutes after flight arrives folks are boarded, and temp starts to drop towards ambient temperature

            base.InitSchedule(reportingIntervalSeconds);

            int deltaTempPreFlight = 5;
            int ambientTemp = 65;
            int lastDepartureHour = 23; //hour 23 is 11pm
            int minutesBetweenFlights = 90;

            TempDataPoint datapoint = new TempDataPoint()
            {
                temp = ambientTemp,
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
                    datapoint.temp += (double) deltaTempPreFlight / (30 * 60 / reportingIntervalSeconds);
                }
                else if (IsWithinPostFlightWindow(i, reportingIntervalSeconds, nextDepartureIntervalNumber))
                {
                    datapoint.temp -= (double) deltaTempPreFlight / (30 * 60 / reportingIntervalSeconds);
                }
                else
                {
                    datapoint.temp = ambientTemp;
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
            if ((intervalNumber * reportingInterval) >= (departureIntervalNumber * reportingInterval) - (30*60) &&
                (intervalNumber * reportingInterval) < (departureIntervalNumber * reportingInterval))
            {
                return true;
            }

            return false;
        }

        private bool IsWithinPostFlightWindow(int intervalNumber, int reportingInterval, int departureIntervalNumber)
        {
            //Post-Flight window lasts from departure to 30 minutes after
            if ((intervalNumber * reportingInterval) >= (departureIntervalNumber * reportingInterval)  &&
                (intervalNumber * reportingInterval) < (departureIntervalNumber * reportingInterval) + (30 * 60))
            {
                return true;
            }

            return false;
        }

        private bool HasPlaneDeparted(int intervalNumber, int reportingInterval, int departureIntervalNumber)
        {
            if (intervalNumber * reportingInterval > departureIntervalNumber * reportingInterval + 30 * 60)
            {
                return true;
            }
            return false;
        }
                
        private class TempDataPoint
        {
            public double temp;
            public DateTime createDate;
            public string deviceId;
        }

    }
}
