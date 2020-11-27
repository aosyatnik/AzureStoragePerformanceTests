using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace PerformanceTests
{
    public class MergedMessageEntity_Option4 : TableEntity
    {
        public string Sensor1 { get; set; }
        public string Sensor2 { get; set; }
        public string Sensor3 { get; set; }
        public string Sensor4 { get; set; }
        public string Sensor5 { get; set; }

        private Dictionary<string, double> _sensor1Dict;

        [IgnoreProperty]
        public Dictionary<string, double> Sensor1Dict
        {
            get
            {
                if (_sensor1Dict is null)
                {
                    _sensor1Dict = JsonConvert.DeserializeObject<Dictionary<string, double>>(Sensor1);
                }
                return _sensor1Dict;
            }
        }

        private Dictionary<string, double> _sensor2Dict;

        [IgnoreProperty]
        public Dictionary<string, double> Sensor2Dict
        {
            get
            {
                if (_sensor2Dict is null)
                {
                    _sensor2Dict = JsonConvert.DeserializeObject<Dictionary<string, double>>(Sensor2);
                }
                return _sensor2Dict;
            }
        }

        private Dictionary<string, double> _sensor3Dict;

        [IgnoreProperty]
        public Dictionary<string, double> Sensor3Dict
        {
            get
            {
                if (_sensor3Dict is null)
                {
                    _sensor3Dict = JsonConvert.DeserializeObject<Dictionary<string, double>>(Sensor3);
                }
                return _sensor3Dict;
            }
        }

        private Dictionary<string, double> _sensor4Dict;

        [IgnoreProperty]
        public Dictionary<string, double> Sensor4Dict
        {
            get
            {
                if (_sensor4Dict is null)
                {
                    _sensor4Dict = JsonConvert.DeserializeObject<Dictionary<string, double>>(Sensor4);
                }
                return _sensor4Dict;
            }
        }

        private Dictionary<string, double> _sensor5Dict;

        [IgnoreProperty]
        public Dictionary<string, double> Sensor5Dict
        {
            get
            {
                if (_sensor5Dict is null)
                {
                    _sensor5Dict = JsonConvert.DeserializeObject<Dictionary<string, double>>(Sensor5);
                }
                return _sensor5Dict;
            }
        }
    }
}
