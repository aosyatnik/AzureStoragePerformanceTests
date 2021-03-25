using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading.Tasks;
using TableStorage.Abstractions.Store;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at https://go.microsoft.com/fwlink/?LinkId=402352&clcid=0x409

namespace PerformanceTests
{
    /// <summary>
    /// An empty page that can be used on its own or navigated to within a Frame.
    /// </summary>
    public sealed partial class MainPage : Page
    {
        private const string connectionString = "CONNECTION_STRING";
        private CloudStorageAccount storageAccount;
        private CloudTableClient tableClient;
        private BlobServiceClient _blobServiceClient;

        public const string CONTAINER_NAME = "deviceid";

        public DateTimeOffset StartDay = new DateTimeOffset(2019, 01, 01, 0, 0, 0, TimeSpan.Zero);
        public DateTimeOffset EndDay = new DateTimeOffset(2019, 01, 01, 0, 0, 0, TimeSpan.Zero);
        public int DaysToAdd = 1;
        public int DayIndex = 0;

        public MainPage()
        {
            this.InitializeComponent();
            storageAccount = CloudStorageAccount.Parse(connectionString);
            tableClient = storageAccount.CreateCloudTableClient();
            _blobServiceClient = new BlobServiceClient(connectionString);

            var container = _blobServiceClient.GetBlobContainers().FirstOrDefault(c => c.Name == CONTAINER_NAME);
            if (container is null)
            {
                _blobServiceClient.CreateBlobContainer(CONTAINER_NAME);
            }
        }

        private void Delete_All_Button_Click(object sender, RoutedEventArgs e)
        {
            foreach (var table in tableClient.ListTables())
                table.DeleteIfExists();
        }

        #region Option 1.1

        private async void Retrive_Option1_1_LongFilter(object sender, RoutedEventArgs e)
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
                        for (var second = 0; second < 60; second++)
                        {
                            rowKeys.Add(StartDay.AddDays(d).AddHours(hour).AddMinutes(minute).AddSeconds(second).ToString("yyyyMMddHHmmss"));
                        }
                }

            var table = tableClient.GetTableReference("option11");

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
                ResultTextBox.Text = $"Option1.1 long filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        private async void Retrive_Option1_1_ShortFilter(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option11");
            var endTime = EndTimePicker.Time;

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var startRowKey = StartDay.Date.ToString("yyyyMMddHHmmss");
                var endRowKey = StartDay.Date.AddHours(endTime.Hours).AddMinutes(endTime.Minutes).ToString("yyyyMMddHHmmss");

                var finalResult = new List<MessageEntity>();
                //var tasks = new List<Task>();

                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "device_id");
                var startRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey);
                var endRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, endRowKey);
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

                //await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option1.1 short filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        #endregion

        #region Option 2.1

        private async void Retrive_Option2_1_LongFilter(object sender, RoutedEventArgs e)
        {
            var rowKeys = new List<string>();

            var endTime = EndTimePicker.Time;
            var days = EndDay.Date.Subtract(StartDay.Date).Days;

            for (var d = 0; d <= days; d++)
                for (var hour = 0; hour <= endTime.Hours; hour++)
                {
                    var endMinutes = hour == endTime.Hours ? endTime.Minutes : 60;
                    for (var minute = 0; minute < endMinutes; minute++)
                        for (var second = 0; second < 60; second++)
                        {
                            rowKeys.Add(StartDay.AddDays(d).AddHours(hour).AddMinutes(minute).AddSeconds(second).ToString("yyyyMMddHHmmss"));
                        }
                }

            var table = tableClient.GetTableReference("option21");

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();

                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, StartDay.Date.ToString("yyyyMMdd"));
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
                ResultTextBox.Text = $"Option2.1 long filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                ResultTextBox.Text = ex.Message;
                Debug.WriteLine(ex.Message);
            }
        }

        private async void Retrive_Option2_1_ShortFilter(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option21");
            var endTime = EndTimePicker.Time;

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var startRowKey = StartDay.Date.ToString("yyyyMMddHHmmss");
                var endRowKey = StartDay.Date.AddHours(endTime.Hours).AddMinutes(endTime.Minutes).ToString("yyyyMMddHHmmss");

                var finalResult = new List<MessageEntity>();
                //var tasks = new List<Task>();

                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, StartDay.Date.ToString("yyyyMMdd"));
                var startRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey);
                var endRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, endRowKey);
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

                //await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option2.1 short filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        #endregion

        #region Option 3.5

        private async void Retrive_Option3_5(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option35");

            var startTime = StartTimePicker.Time;
            var endTime = EndTimePicker.Time;
            var days = EndDay.Date.Subtract(StartDay.Date).Days;

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();

                var hourFilters = "";

                for (var d = 0; d <= days; d++)
                    for (var hour = 0; hour <= endTime.Hours; hour++)
                    {
                        var endMinutes = d == days && hour == endTime.Hours ? endTime.Minutes : 60;

                        if (endMinutes == 60) // full hour
                        {
                            var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, StartDay.AddDays(d).AddHours(hour).ToString("yyyyMMddHH"));
                            var query = new TableQuery<MessageEntity>().Where(partitionFilter);

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

                            /*var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, StartDay.AddDays(d).AddHours(hour).ToString("yyyyMMddHH"));
                            if (hour != endTime.Hours - 1)
                                partitionFilter += " " + TableOperators.Or + " ";

                            hourFilters += partitionFilter;*/
                        }
                        else // end minutes
                        {
                            var startRowKey = StartDay.Date.AddDays(d).AddHours(hour).ToString("yyyyMMddHHmmssffff");
                            var endRowKey = StartDay.Date.AddDays(d).AddHours(hour).AddMinutes(endTime.Minutes).ToString("yyyyMMddHHmmssffff");

                            var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, StartDay.AddDays(d).AddHours(hour).ToString("yyyyMMddHH"));
                            var startRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey);
                            var endRowsFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRowKey);
                            var rowsFilter = TableQuery.CombineFilters(startRowsFilter, TableOperators.And, endRowsFilter);
                            var finalFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowsFilter);
                            var query = new TableQuery<MessageEntity>().Where(finalFilter);

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
                    }

                if (hourFilters != "")
                {
                    var query = new TableQuery<MessageEntity>().Where(hourFilters);
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
                ResultTextBox.Text = $"Option3.5: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                ResultTextBox.Text = ex.Message;
                Debug.WriteLine(ex.Message);
            }
        }

        #endregion

        #region Json cache

        private async void Upload_Json_Click(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option5");

            try
            {
                var finalResult = new List<MergedMessageEntity_Option5>();
                var tasks = new List<Task>();

                for (var hour = 0; hour <= 23; hour++)
                {
                    var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, StartDay.AddHours(hour).ToString("yyyyMMddHH"));
                    var query = new TableQuery<MergedMessageEntity_Option5>().Where(partitionFilter);

                    tasks.Add(Task.Run(async () =>
                    {
                        //Stopwatch sw1 = new Stopwatch();
                        //sw1.Start();

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

                var fileName = StartDay.Date.ToString("yyyyMMdd");

                var stream = new MemoryStream();
                using (var streamWriter = new StreamWriter(stream: stream, encoding: Encoding.UTF8, bufferSize: 4096, leaveOpen: true)) // last parameter is important
                using (var jsonWriter = new JsonTextWriter(streamWriter))
                {
                    var serializer = new JsonSerializer();
                    serializer.Serialize(jsonWriter, finalResult);
                    streamWriter.Flush();
                    stream.Seek(0, SeekOrigin.Begin);
                }

                var blobClient = new BlobClient(connectionString, CONTAINER_NAME, fileName);
                await blobClient.DeleteIfExistsAsync();
                await blobClient.UploadAsync(stream);

                ResultTextBox.Text = $"Uploaded json file: {fileName}";
            }
            catch (Exception ex)
            {
                ResultTextBox.Text = ex.Message;
                Debug.WriteLine(ex.Message);
            }
        }


        private async void Download_Json_Click(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            try
            {
                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();

                var days = EndDay.Date.Subtract(StartDay.Date).Days;

                Stopwatch sw = new Stopwatch();
                sw.Start();

                for (var d = 0; d <= days; d++)
                {
                    var copy = d;
                    tasks.Add(Task.Run(async () =>
                    {
                        var fileName = StartDay.Date.AddDays(copy).ToString("yyyyMMdd");
                        var blobClient = new BlobClient(connectionString, CONTAINER_NAME, fileName);

                        try
                        {
                            if (!await blobClient.ExistsAsync())
                            {
                                return;
                            }
                        }
                        catch
                        {
                            return;
                        }

                        var download = await blobClient.DownloadAsync();

                        var mergedMessages = download.Value.Content.Deserialize<IEnumerable<MergedMessageEntity_Option5>>();
                        var messages = mergedMessages.SelectMany(merged =>
                        {
                            var mm = new List<MessageEntity>();
                            for (var sec = 0; sec < 60; sec++)
                            {
                                var m = new MessageEntity();
                                var value = merged.GetValue(sec);
                                m.Sensor1 = value.Sensor1;
                                m.Sensor2 = value.Sensor2;
                                m.Sensor3 = value.Sensor3;
                                m.Sensor4 = value.Sensor4;
                                m.Sensor5 = value.Sensor5;
                                mm.Add(m);
                            }

                            return mm;
                        });

                        lock (finalResult)
                        {
                            finalResult.AddRange(messages);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                sw.Stop();
                ResultTextBox.Text = $"Downloaded from json cache: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                ResultTextBox.Text = ex.Message;
                Debug.WriteLine(ex.Message);
            }
        }

        #endregion
    }
}
