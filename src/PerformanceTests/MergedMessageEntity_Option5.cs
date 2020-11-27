using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;

namespace PerformanceTests
{
    public class MergedMessageEntity_Option5 : TableEntity
    {
        public string Second_0 { get; set; }
        public string Second_1 { get; set; }
        public string Second_2 { get; set; }
        public string Second_3 { get; set; }
        public string Second_4 { get; set; }
        public string Second_5 { get; set; }
        public string Second_6 { get; set; }
        public string Second_7 { get; set; }
        public string Second_8 { get; set; }
        public string Second_9 { get; set; }
        public string Second_10 { get; set; }
        public string Second_11 { get; set; }
        public string Second_12 { get; set; }
        public string Second_13 { get; set; }
        public string Second_14 { get; set; }
        public string Second_15 { get; set; }
        public string Second_16 { get; set; }
        public string Second_17 { get; set; }
        public string Second_18 { get; set; }
        public string Second_19 { get; set; }
        public string Second_20 { get; set; }
        public string Second_21 { get; set; }
        public string Second_22 { get; set; }
        public string Second_23 { get; set; }
        public string Second_24 { get; set; }
        public string Second_25 { get; set; }
        public string Second_26 { get; set; }
        public string Second_27 { get; set; }
        public string Second_28 { get; set; }
        public string Second_29 { get; set; }
        public string Second_30 { get; set; }
        public string Second_31 { get; set; }
        public string Second_32 { get; set; }
        public string Second_33 { get; set; }
        public string Second_34 { get; set; }
        public string Second_35 { get; set; }
        public string Second_36 { get; set; }
        public string Second_37 { get; set; }
        public string Second_38 { get; set; }
        public string Second_39 { get; set; }
        public string Second_40 { get; set; }
        public string Second_41 { get; set; }
        public string Second_42 { get; set; }
        public string Second_43 { get; set; }
        public string Second_44 { get; set; }
        public string Second_45 { get; set; }
        public string Second_46 { get; set; }
        public string Second_47 { get; set; }
        public string Second_48 { get; set; }
        public string Second_49 { get; set; }
        public string Second_50 { get; set; }
        public string Second_51 { get; set; }
        public string Second_52 { get; set; }
        public string Second_53 { get; set; }
        public string Second_54 { get; set; }
        public string Second_55 { get; set; }
        public string Second_56 { get; set; }
        public string Second_57 { get; set; }
        public string Second_58 { get; set; }
        public string Second_59 { get; set; }

        public Second GetValue(int second)
        {
            var json = this.GetPropValue($"Second_{second}") as string;
            return JsonConvert.DeserializeObject<Second>(json);
        }
    }

    public class Second
    {
        public double Sensor1 { get; set; }
        public double Sensor2 { get; set; }
        public double Sensor3 { get; set; }
        public double Sensor4 { get; set; }
        public double Sensor5 { get; set; }
    }
}
