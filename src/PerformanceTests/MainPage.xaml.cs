using Microsoft.Azure.Cosmos.Table;
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
        private const string connectionString = "DefaultEndpointsProtocol=https;AccountName=pmsysperformancetests;AccountKey=+86NsTIPwT27tA+5/w4EktnU5GAGJZtTBrFGmsapBxDafd7AJwazyRm2+bekAaYaSStobQ2il9ZW8JCjyVWdNw==;EndpointSuffix=core.windows.net";
        private CloudStorageAccount storageAccount;
        private CloudTableClient tableClient;

        public DateTimeOffset Day = new DateTimeOffset(2019, 01, 01, 0, 0, 0, TimeSpan.Zero);
        public int DaysToAdd = 180;
        public int DayIndex = 0;

        public MainPage()
        {
            this.InitializeComponent();
            storageAccount = CloudStorageAccount.Parse(connectionString);
            tableClient = storageAccount.CreateCloudTableClient();
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
                var messages = GenerateMessagesEachSecond(Day.Date.AddDays(day), "Option1");
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
        private async void Retrive_Option1(object sender, RoutedEventArgs e)
        {
            var endTime = EndTimePicker.Time;

            var rowKeys = new List<string>();
            for (var hour = 0; hour <= endTime.Hours; hour++)
            {
                var endMinutes = hour == endTime.Hours ? endTime.Minutes : 60;
                for (var minute = 0; minute < endMinutes; minute++)
                    for (var second = 0; second < 60; second++)
                    {
                        rowKeys.Add(Day.AddHours(hour).AddMinutes(minute).AddSeconds(second).ToString("yyyyMMddHHmmss"));
                    }
            }

            var table = tableClient.GetTableReference("option1");

            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "device_id");
                var rowsFilter = new StringBuilder();
                var finalResult = new List<MessageEntity>();
                var tasks = new List<Task>();
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
                                finalResult.AddRange(seg);
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
                            finalResult.AddRange(seg);
                        }
                        while (token != null);
                    }));
                }

                await Task.WhenAll(tasks);

                sw.Stop();

                ResultTextBox.Text = $"Found {finalResult.Count} records. It took {String.Format("{0:0.00000}", sw.Elapsed.TotalSeconds)} seconds.";
            }
            catch (Exception ex)
            {
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
