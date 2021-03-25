using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Windows.UI.Xaml;

namespace PerformanceTests
{
    public sealed partial class MainPage
    {
        private async void Retrive_Option3(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var startTime = StartTimePicker.Time;
            var endTime = EndTimePicker.Time;
            var days = EndDay.Date.Subtract(StartDay.Date).Days;

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();

                var hourFilters = new List<string>();

                for (var d = 0; d <= days; d++)
                    for (var hour = 0; hour <= endTime.Hours; hour++)
                    {
                        var endMinutes = d == days && hour == endTime.Hours ? endTime.Minutes : 60;

                        if (endMinutes == 60) // full hour
                        {
                            var h = StartDay.Date.AddDays(d).AddHours(hour);
                            for (var i = 0; i < 4; i++)
                            {
                                var min15_block = h.AddMinutes(i * 15).ToString("yyyyMMddHHmm");
                                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, min15_block);
                                var query = new TableQuery<MessageEntity>().Where(partitionFilter);

                                tasks.Add(Task.Run(async () =>
                                {
                                    Stopwatch sw1 = new Stopwatch();
                                    sw1.Start();

                                    var table = tableClient.GetTableReference("option3");
                                    var token = new TableContinuationToken();
                                    do
                                    {
                                        var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                                        token = seg.ContinuationToken;
                                        lock (finalResult)
                                        {
                                            finalResult.AddRange(seg);
                                        }
                                    }
                                    while (token != null);

                                    sw1.Stop();
                                    Debug.WriteLine($"Retrieve 15 min block takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}. Query: {partitionFilter}");
                                }));
                            }
                        }

                        // ---------- Get 4 15 min blocks as 1 call. -----------------

                        /*var endMinutes = hour == endTime.Hours ? endTime.Minutes : 60;
                        var patitionFilters = "";
                        if (endMinutes == 60) // full hour
                        {
                            var h = Day.Date.AddHours(hour);
                            for (var i = 0; i < 4; i++)
                            {
                                var min15_block = h.AddMinutes(i * 15).ToString("yyyyMMddHHmm");
                                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, min15_block);
                                if (i != 3)
                                    partitionFilter += " " + TableOperators.Or + " ";

                                patitionFilters += partitionFilter;
                            }


                            var query = new TableQuery<MessageEntity>().Where(patitionFilters);

                            tasks.Add(Task.Run(async () =>
                            {
                                Stopwatch sw1 = new Stopwatch();
                                sw1.Start();

                                var table = tableClient.GetTableReference("option3");
                                var token = new TableContinuationToken();
                                do
                                {
                                    var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                                    token = seg.ContinuationToken;
                                    lock (finalResult)
                                    {
                                        finalResult.AddRange(seg);
                                    }
                                }
                                while (token != null);

                                sw1.Stop();
                                Debug.WriteLine($"Retrieve 1 hour takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}. Query: {patitionFilters}");
                            }));
                        }*/


                        // ---------- Get all hours as 1 call. -----------------

                        /*var endMinutes = hour == endTime.Hours ? endTime.Minutes : 60;
                        var hourfilter = "";
                        if (endMinutes == 60)
                        {
                            var h = Day.Date.AddHours(hour);
                            for (var i = 0; i < 4; i++)
                            {
                                var min15_block = h.AddMinutes(i * 15).ToString("yyyyMMddHHmm");
                                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, min15_block);
                                if (i != 3)
                                    partitionFilter += " " + TableOperators.Or + " ";

                                hourfilter += partitionFilter;
                            }

                            hourFilters.Add(hourfilter);
                        }*/

                        else // + extra minutes
                        {
                            var min15_blocks = endMinutes / 15;
                            var h = StartDay.Date.AddDays(d).AddHours(hour);

                            for (var i = 0; i < min15_blocks; i++)
                            {
                                var min15_block = h.AddMinutes(i * 15).ToString("yyyyMMddHHmm");
                                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, min15_block);
                                var query = new TableQuery<MessageEntity>().Where(partitionFilter);

                                tasks.Add(Task.Run(async () =>
                                {
                                    Stopwatch sw1 = new Stopwatch();
                                    sw1.Start();

                                    var token = new TableContinuationToken();
                                    var table = tableClient.GetTableReference("option3");
                                    do
                                    {
                                        var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                                        token = seg.ContinuationToken;
                                        lock (finalResult)
                                        {
                                            finalResult.AddRange(seg);
                                        }
                                    }
                                    while (token != null);

                                    sw1.Stop();
                                    Debug.WriteLine($"Retrieve 15 min block {min15_block} takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}");
                                }));
                            }

                            var leftMinutes = endMinutes % 15;
                            if (leftMinutes > 0)
                            {
                                h = h.AddMinutes(min15_blocks * 15);
                                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, h.ToString("yyyyMMddHHmm"));
                                var rowKeys = new List<string>();

                                //Stopwatch sw1 = new Stopwatch();
                                //sw1.Start();

                                for (var i = 0; i <= leftMinutes; i++)
                                    for (var second = 0; second < 60; second++)
                                    {
                                        rowKeys.Add(h.AddMinutes(i).AddSeconds(second).ToString("yyyyMMddHHmmss"));
                                    }

                                //sw1.Stop();
                                //Debug.WriteLine($"Key generation takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}");

                                var rowsFilter = new StringBuilder();
                                var k = 0;
                                for (var i = 0; i < rowKeys.Count; i++)
                                {
                                    k++;
                                    var r = rowKeys[i];
                                    rowsFilter.Append(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, r));

                                    if (i != rowKeys.Count - 1 && k != 700)
                                        rowsFilter.Append(" " + TableOperators.Or + " ");

                                    if (k == 700)
                                    {
                                        var comFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowsFilter.ToString());
                                        var q = new TableQuery<MessageEntity>().Where(comFilter);

                                        tasks.Add(Task.Run(async () =>
                                        {
                                            Stopwatch sw1 = new Stopwatch();
                                            sw1.Start();

                                            var token = new TableContinuationToken();
                                            var table = tableClient.GetTableReference("option3");
                                            do
                                            {
                                                var seg = await table.ExecuteQuerySegmentedAsync(q, token);
                                                token = seg.ContinuationToken;
                                                finalResult.AddRange(seg);
                                            }
                                            while (token != null);

                                            sw1.Stop();
                                            Debug.WriteLine($"Retrieve left minutes {leftMinutes} takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}");
                                        }));

                                        rowsFilter.Clear();
                                        k = 0;
                                    }
                                }


                                var combinedFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowsFilter.ToString());
                                var query = new TableQuery<MessageEntity>().Where(combinedFilter);

                                tasks.Add(Task.Run(async () =>
                                {
                                    Stopwatch sw1 = new Stopwatch();
                                    sw1.Start();

                                    var token = new TableContinuationToken();
                                    var table = tableClient.GetTableReference("option3");
                                    do
                                    {
                                        var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                                        token = seg.ContinuationToken;
                                        finalResult.AddRange(seg);
                                    }
                                    while (token != null);

                                    sw1.Stop();
                                    Debug.WriteLine($"Retrieve left minutes {leftMinutes} takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}");
                                }));

                            }
                        }
                    }

                // ---------- Get all hours as 1 call. -----------------

                /*var hoursFinalFilter = new StringBuilder();
                for (var i = 0; i < hourFilters.Count; i++)
                {
                    var hourFilter = hourFilters[i];
                    hoursFinalFilter.Append(hourFilter);

                    if (i != hourFilters.Count - 1)
                        hoursFinalFilter.Append(" " + TableOperators.Or + " ");
                }

                if (hourFilters.Count > 0)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        var query = new TableQuery<MessageEntity>().Where(hoursFinalFilter.ToString());

                        Stopwatch sw1 = new Stopwatch();
                        sw1.Start();

                        var table = tableClient.GetTableReference("option3");
                        var token = new TableContinuationToken();
                        do
                        {
                            var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                            token = seg.ContinuationToken;
                            lock (finalResult)
                            {
                                finalResult.AddRange(seg);
                            }
                        }
                        while (token != null);

                        sw1.Stop();
                        Debug.WriteLine($"Retrieve {hourFilters.Count} hours takes: {String.Format("{0:0.00000}", sw1.Elapsed.TotalSeconds)}.");
                    }));
                }*/

                await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option3: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                ResultTextBox.Text = ex.Message;
                Debug.WriteLine(ex.Message);
            }
        }
    }
}
