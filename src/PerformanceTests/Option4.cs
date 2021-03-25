using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Windows.UI.Xaml;

namespace PerformanceTests
{
    public sealed partial class MainPage
    {
        private async void Retrive_Option4(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option4");
            var startTime = StartTimePicker.Time;
            var endTime = EndTimePicker.Time;
            var days = EndDay.Date.Subtract(StartDay.Date).Days;

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();

                for (var d = 0; d <= days; d++)
                    for (var hour = 0; hour <= endTime.Hours; hour++)
                    {
                        var endMinutes = d == days && hour == endTime.Hours ? endTime.Minutes : 60;

                        if (endMinutes == 60) // full hour
                        {
                            var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, StartDay.AddDays(d).AddHours(hour).ToString("yyyyMMddHH"));
                            var query = new TableQuery<MergedMessageEntity_Option4>().Where(partitionFilter);

                            tasks.Add(Task.Run(async () =>
                            {
                                //Stopwatch sw1 = new Stopwatch();
                                //sw1.Start();

                                var token = new TableContinuationToken();
                                do
                                {
                                    var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                                    token = seg.ContinuationToken;


                                    var messages = seg.SelectMany(merged =>
                                    {
                                        var mm = new List<MessageEntity>();

                                        for (var sec = 0; sec < 60; sec++)
                                        {
                                            var m = new MessageEntity();
                                            m.Sensor1 = merged.Sensor1Dict[$"{sec}"];
                                            m.Sensor2 = merged.Sensor2Dict[$"{sec}"];
                                            m.Sensor3 = merged.Sensor3Dict[$"{sec}"];
                                            m.Sensor4 = merged.Sensor4Dict[$"{sec}"];
                                            m.Sensor5 = merged.Sensor5Dict[$"{sec}"];
                                            mm.Add(m);
                                        }

                                        return mm;
                                    });

                                    lock (finalResult)
                                    {
                                        finalResult.AddRange(messages);
                                    }
                                }
                                while (token != null);

                                //sw1.Stop();
                                //Debug.WriteLine($"Retrieve 1 hour block takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}");
                            }));
                        }
                        else // left minutes
                        {
                            var h = StartDay.AddDays(d).AddHours(hour);
                            var rowKeys = new List<string>();
                            var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, h.ToString("yyyyMMddHH"));
                            for (var i = 0; i <= endMinutes; i++)
                            {
                                rowKeys.Add(h.AddMinutes(i).ToString("yyyyMMddHHmm"));
                            }

                            var rowsFilter = new StringBuilder();
                            for (var i = 0; i < rowKeys.Count; i++)
                            {
                                var r = rowKeys[i];
                                rowsFilter.Append(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, r));

                                if (i != rowKeys.Count - 1)
                                    rowsFilter.Append(" " + TableOperators.Or + " ");
                            }

                            if (rowsFilter.Length > 0)
                            {
                                var combinedFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowsFilter.ToString());
                                var query = new TableQuery<MergedMessageEntity_Option4>().Where(combinedFilter);

                                tasks.Add(Task.Run(async () =>
                                {
                                    //Stopwatch sw1 = new Stopwatch();
                                    //sw1.Start();

                                    var token = new TableContinuationToken();
                                    do
                                    {
                                        var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                                        token = seg.ContinuationToken;


                                        var messages = seg.SelectMany(merged =>
                                        {
                                            var mm = new List<MessageEntity>();

                                            for (var sec = 0; sec < 60; sec++)
                                            {
                                                var m = new MessageEntity();
                                                m.Sensor1 = merged.Sensor1Dict[$"{sec}"];
                                                m.Sensor2 = merged.Sensor2Dict[$"{sec}"];
                                                m.Sensor3 = merged.Sensor3Dict[$"{sec}"];
                                                m.Sensor4 = merged.Sensor4Dict[$"{sec}"];
                                                m.Sensor5 = merged.Sensor5Dict[$"{sec}"];
                                                mm.Add(m);
                                            }

                                            return mm;
                                        });

                                        lock (finalResult)
                                        {
                                            finalResult.AddRange(messages);
                                        }
                                    }
                                    while (token != null);

                                    //sw1.Stop();
                                    //Debug.WriteLine($"Retrieve left minutes {leftMinutes} takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}");
                                }));
                            }
                        }
                    }


                await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option4: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                ResultTextBox.Text = ex.Message;
                Debug.WriteLine(ex.Message);
            }
        }
    }
}
