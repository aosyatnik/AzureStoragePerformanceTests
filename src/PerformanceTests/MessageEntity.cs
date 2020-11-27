using Microsoft.Azure.Cosmos.Table;
using System;

namespace PerformanceTests
{
    public class MessageEntity : TableEntity
    {
        public double Sensor1 { get; set; }
        public double Sensor2 { get; set; }
        public double Sensor3 { get; set; }
        public double Sensor4 { get; set; }
        public double Sensor5 { get; set; }

        private readonly DateTime generationTime;

        public MessageEntity()
        {
        }

        public MessageEntity(DateTime time, string option)
        {
            generationTime = time;

            var random = new Random();

            Sensor1 = random.NextDouble();
            Sensor2 = random.NextDouble();
            Sensor3 = random.NextDouble();
            Sensor4 = random.NextDouble();
            Sensor5 = random.NextDouble();

            switch (option)
            {
                case "Option1":
                    Option1();
                    break;

                case "Option2":
                    Option2();
                    break;

                default:
                    throw new Exception("Unknown option");
            }
        }

        /// <summary>
        /// 1 table
        /// - partion key: device ID
        /// - rowkey: timestamp
        /// </summary>
        public void Option1()
        {
            PartitionKey = "device_id";
            RowKey = generationTime.ToString("yyyyMMddHHmmss");
        }

        /// <summary>
        /// 1 table
        /// - partion key: day
        /// - rowkey: timestamp
        /// </summary>
        public void Option2()
        {
            PartitionKey = generationTime.ToString("yyyyMMdd");
            RowKey = generationTime.ToString("yyyyMMddHHmmss");
        }
    }
}
