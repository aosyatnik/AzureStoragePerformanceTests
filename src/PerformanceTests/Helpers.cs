using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PerformanceTests
{
    public static class Helpers
    {
        public static object GetPropValue(this object src, string propName)
        {
            return src.GetType().GetProperty(propName).GetValue(src, null);
        }

        public static T Deserialize<T>(this Stream stream)
        {
            var serializer = new JsonSerializer();
            var sr = new StreamReader(stream);
            var jr = new JsonTextReader(sr);
            return serializer.Deserialize<T>(jr);
        }
    }
}
