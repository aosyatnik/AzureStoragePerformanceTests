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
        private async void Retrive_Option1_LongFilter(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var rowKeys = new List<string>();

            var endTime = EndTimePicker.Time;
            var days = EndDay.Date.Subtract(StartDay.Date).Days;

            for (var d = 0; d <= days; d++)
                for (var hour = 0; hour <= endTime.Hours; hour++)
                {
                    var endMinutes = hour == endTime.Hours ? endTime.Minutes : 60;
                    for (var minute = 0; minute < endMinutes; minute++)
                        for (var second = 0; second <= 60; second++)
                        {
                            rowKeys.Add(StartDay.AddDays(d).AddHours(hour).AddMinutes(minute).AddSeconds(second).ToString("yyyyMMddHHmmss"));
                        }
                }

            var table = tableClient.GetTableReference("option1");

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();

                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "device_id");
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
                        var combinedFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowsFilter.ToString());
                        var query = new TableQuery<MessageEntity>().Where(combinedFilter);

                        tasks.Add(Task.Run(async () =>
                        {
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
                        }));

                        rowsFilter.Clear();
                        k = 0;
                    }
                }

                if (rowsFilter.Length > 0)
                {
                    var combinedFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowsFilter.ToString());
                    var query = new TableQuery<MessageEntity>().Where(combinedFilter);

                    tasks.Add(Task.Run(async () =>
                    {
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
                    }));
                }

                await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option1 long filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        private async void Retrive_Option1_ShortFilter(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option1");
            var endTime = EndTimePicker.Time;

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var startRowKey = StartDay.Date.ToString("yyyyMMddHHmmss");
                var endRowKey = StartDay.Date.AddHours(endTime.Hours).AddMinutes(endTime.Minutes).ToString("yyyyMMddHHmmss");

                var finalResult = new List<MessageEntity>();

                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "device_id");
                var startRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey);
                var endRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRowKey);
                var rowsFilter = TableQuery.CombineFilters(startRowsFilter, TableOperators.And, endRowsFilter);
                var finalFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowsFilter);
                var query = new TableQuery<MessageEntity>().Where(finalFilter);

                var token = new TableContinuationToken();
                do
                {
                    var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                    token = seg.ContinuationToken;
                    finalResult.AddRange(seg);
                }
                while (token != null);

                sw.Stop();
                ResultTextBox.Text = $"Option1 short filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }
    }
}
