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
        private CloudStorageAccount storageAccount;// => CloudStorageAccount.Parse(connectionString);
        private CloudTableClient tableClient;// => storageAccount.CreateCloudTableClient();
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

        #region Option 1

        private async void Generate_Option1(object sender, RoutedEventArgs e)
        {
            if (!DatePicker.SelectedDate.HasValue)
                return;

            var table = tableClient.GetTableReference("option1");
            try
            {
                table.CreateIfNotExists();
            }
            catch (Exception)
            {
            }


            for (var day = 0; day < DaysToAdd; day++)
            {
                DayIndex = day;
                var messages = GenerateMessagesEachSecond(StartDay.Date.AddDays(day), "Option1");
                var tasks = new List<Task>();

                try
                {
                    var operation = new TableBatchOperation();
                    foreach (var m in messages)
                    {
                        operation.Insert(m);
                        if (operation.Count == 100)
                        {
                            tasks.Add(table.ExecuteBatchAsync(operation));
                            operation = new TableBatchOperation();
                        }
                    }

                    if (operation.Count > 0)
                        tasks.Add(table.ExecuteBatchAsync(operation));

                    await Task.WhenAll();
                }
                catch (Exception ex)
                {
                    ResultTextBox.Text = ex.Message;
                    Debug.WriteLine(ex.Message);
                }
            }
        }
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
                        for (var second = 0; second < 60; second++)
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
                //var tasks = new List<Task>();

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

                //await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option1 short filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        private async void Retrive_Option1_LongBatch(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var endTime = EndTimePicker.Time;

            var rowKeys = new List<string>();
            for (var hour = 0; hour <= endTime.Hours; hour++)
            {
                var endMinutes = hour == endTime.Hours ? endTime.Minutes : 60;
                for (var minute = 0; minute < endMinutes; minute++)
                    for (var second = 0; second < 60; second++)
                    {
                        rowKeys.Add(StartDay.AddHours(hour).AddMinutes(minute).AddSeconds(second).ToString("yyyyMMddHHmmss"));
                    }
            }

            var table = tableClient.GetTableReference("option1");

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();

                var operation = new TableBatchOperation();

                for (var i = 0; i < rowKeys.Count; i++)
                {
                    var k = rowKeys[i];
                    operation.Add(TableOperation.Retrieve("device_id", k));
                    if (operation.Count == 100)
                    {
                        tasks.Add(Task.Run(async () =>
                       {
                           var res = await table.ExecuteBatchAsync(operation);
                           finalResult.AddRange(res.Select(r => r.Result as MessageEntity));
                       }));
                        operation = new TableBatchOperation();
                    }
                }

                if (operation.Count > 0)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        var res = await table.ExecuteBatchAsync(operation);
                        finalResult.AddRange(res.Select(r => r.Result as MessageEntity));
                    }));
                }

                await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option1 long batch: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        #endregion

        #region Option 2

        private async void Generate_Option2(object sender, RoutedEventArgs e)
        {
            if (!DatePicker.SelectedDate.HasValue)
                return;

            var table = tableClient.GetTableReference("option2");
            try
            {
                table.CreateIfNotExists();
            }
            catch (Exception)
            {
            }

            for (var day = 0; day < DaysToAdd; day++)
            {
                DayIndex = day;
                var messages = GenerateMessagesEachSecond(StartDay.Date.AddDays(day), "Option2");
                var tasks = new List<Task>();

                try
                {
                    var operation = new TableBatchOperation();
                    foreach (var m in messages)
                    {
                        operation.Insert(m);
                        if (operation.Count == 100)
                        {
                            tasks.Add(table.ExecuteBatchAsync(operation));
                            operation = new TableBatchOperation();
                        }
                    }

                    if (operation.Count > 0)
                        tasks.Add(table.ExecuteBatchAsync(operation));

                    await Task.WhenAll();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex.Message);
                }
            }
        }

        private async void Retrive_Option2_LongFilter(object sender, RoutedEventArgs e)
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

            var table = tableClient.GetTableReference("option2");

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
                ResultTextBox.Text = $"Option2 long filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                ResultTextBox.Text = ex.Message;
                Debug.WriteLine(ex.Message);
            }
        }

        private async void Retrive_Option2_ShortFilter(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option2");
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

                //await Task.WhenAll(tasks);
                sw.Stop();
                ResultTextBox.Text = $"Option2 short filter: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        #endregion

        #region Option 3

        private async void Retrive_Option3(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            //var table = tableClient.GetTableReference("option3");
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

        #endregion

        #region Option 4

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

        #endregion

        #region Option 5

        private async void Retrive_Option5(object sender, RoutedEventArgs e)
        {
            ResultTextBox.Text = "";

            var table = tableClient.GetTableReference("option5");
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

                                    var messages = seg.SelectMany(merged =>
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
                                var query = new TableQuery<MergedMessageEntity_Option5>().Where(combinedFilter);

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
                ResultTextBox.Text = $"Option 5: Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
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

        private List<MessageEntity> GenerateMessagesEachSecond(DateTime selectedDate, string option)
        {
            List<MessageEntity> messages = new List<MessageEntity>();
            for (var hour = 0; hour < 24; hour++)
                for (var minute = 0; minute < 60; minute++)
                    for (var second = 0; second < 60; second++)
                    {
                        var message = new MessageEntity(selectedDate.AddHours(hour).AddMinutes(minute).AddSeconds(second), option);
                        messages.Add(message);
                    }

            return messages;
        }
    }
}
