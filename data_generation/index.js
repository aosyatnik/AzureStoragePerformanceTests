// Option #: partition key, row key
// Option 1: device_id, yyyyMMDDHHmmss
// Option 2: yyyyMMDD, yyyyMMDDHHmmss
// Option 3: 15 min block yyyyMMDDHHmm, yyyyMMDDHHmmss
// Option 4: merged data yyyyMMDDHH, yyyyMMDDHHmm; Sensor value is column
// Option 5: merged data yyyyMMDDHH, yyyyMMDDHHmm; Second value is column

const option1File = 'option1.csv';
const option2File = 'option2.csv';
const option3File = 'option3.csv';
const option4File = 'option4.csv';
const option5File = 'option5.csv';

// remove old files
const fs = require('fs')
try {
    if (fs.existsSync(option1File))
        fs.unlinkSync(option1File);

    if (fs.existsSync(option2File))
        fs.unlinkSync(option2File);

    if (fs.existsSync(option3File))
        fs.unlinkSync(option3File);

    if (fs.existsSync(option4File))
        fs.unlinkSync(option4File);

    if (fs.existsSync(option5File))
        fs.unlinkSync(option5File);

} catch (err) {
    console.error(err)
}

// ---- data generation ----

const createCsvWriter = require('csv-writer').createArrayCsvWriter;
const moment = require('moment');

const option1 = createCsvWriter({
    path: option1File,
    header: ['PartitionKey', 'RowKey', 'Sensor1', 'Sensor2', 'Sensor3', 'Sensor4', 'Sensor5']
});

const option2 = createCsvWriter({
    path: option2File,
    header: ['PartitionKey', 'RowKey', 'Sensor1', 'Sensor2', 'Sensor3', 'Sensor4', 'Sensor5']
});

const option3 = createCsvWriter({
    path: option3File,
    header: ['PartitionKey', 'RowKey', 'Sensor1', 'Sensor2', 'Sensor3', 'Sensor4', 'Sensor5']
});

let headers_4 = [];
headers_4.push('PartitionKey');
headers_4.push('RowKey');
for (let k = 0; k < 60; k++) {
    headers_4.push('Sensor1_' + k);
    headers_4.push('Sensor2_' + k);
    headers_4.push('Sensor3_' + k);
    headers_4.push('Sensor4_' + k);
    headers_4.push('Sensor5_' + k);
}

const option4 = createCsvWriter({
    path: option4File,
    header: ['PartitionKey', 'RowKey', 'Sensor1', 'Sensor2', 'Sensor3', 'Sensor4', 'Sensor5'],
});

let option5_header = [];
option5_header.push('PartitionKey');
option5_header.push('RowKey');
for (let k = 0; k < 60; k++) {
    option5_header.push('Second_' + k);
}

const option5 = createCsvWriter({
    path: option5File,
    header: option5_header,
});

/*const option3_5 = createCsvWriter({
    path: 'option3_5.csv',
    header: ['PartitionKey', 'RowKey', 'Sensor1', 'Sensor2', 'Sensor3', 'Sensor4', 'Sensor5'],
    append: true
});*/

var startingDate = new Date(2019, 00, 01);
var daysToAdd = 180;

(async () => {
    var min15_block = '';

    let sensor1 = 0.1;
    let sensor2 = 0.2;
    let sensor3 = 0.3;
    let sensor4 = 0.4;
    let sensor5 = 0.5;

    let option1_Records = [];
    let option2_Records = [];
    let option3_Records = [];
    let option4_Records = [];
    let option5_Records = [];
    let option3_5_Records = [];

    for (let day = 0; day < daysToAdd; day++) {
        for (let hour = 0; hour < 24; hour++)
            for (let minute = 0; minute < 60; minute++) {

                if (minute % 15 === 0) {
                    min15_block = moment(startingDate).add(day, 'days').add(hour, 'hours').add(minute, 'minutes').format('yyyyMMDDHHmm');
                }

                let date = moment(startingDate).add(day, 'days').add(hour, 'hours').add(minute, 'minutes');

                var line4 = [];
                line4.push(date.format('yyyyMMDDHH'));
                line4.push(date.format('yyyyMMDDHHmm'));

                var line5 = [];
                line5.push(date.format('yyyyMMDDHH'));
                line5.push(date.format('yyyyMMDDHHmm'));

                let sensor1_merged = {};
                let sensor2_merged = {};
                let sensor3_merged = {};
                let sensor4_merged = {};
                let sensor5_merged = {};

                for (let second = 0; second < 60; second++) {

                    let date = moment(startingDate).add(day, 'days').add(hour, 'hours').add(minute, 'minutes').add(second, 'seconds');
                    //console.log(date.toString() + "  " + date.format('yyyyMMDDHHmmss'));


                    option1_Records.push(['device_id', date.format('yyyyMMDDHHmmss'), sensor1, sensor2, sensor3, sensor4, sensor5]);
                    option2_Records.push([date.format('yyyyMMDD'), date.format('yyyyMMDDHHmmss'), sensor1, sensor2, sensor3, sensor4, sensor5]);
                    option3_Records.push([min15_block, date.format('yyyyMMDDHHmmss'), sensor1, sensor2, sensor3, sensor4, sensor5]);

                    /*line.push(sensor1);
                    line.push(sensor2);
                    line.push(sensor3);
                    line.push(sensor4);
                    line.push(sensor5);*/

                    // option 4
                    sensor1_merged[second] = sensor1;
                    sensor2_merged[second] = sensor2;
                    sensor3_merged[second] = sensor3;
                    sensor4_merged[second] = sensor4;
                    sensor5_merged[second] = sensor5;

                    // option 5
                    let sensor_merged = {};
                    sensor_merged["sensor1"] = sensor1;
                    sensor_merged["sensor2"] = sensor2;
                    sensor_merged["sensor3"] = sensor3;
                    sensor_merged["sensor4"] = sensor4;
                    sensor_merged["sensor5"] = sensor5;
                    line5.push(JSON.stringify(sensor_merged));

                    //option3_5_Records.push([date.format('yyyyMMDDHH'), date.format('yyyyMMDDHHmmss'), sensor1, sensor2, sensor3, sensor4, sensor5]);
                }

                line4.push(JSON.stringify(sensor1_merged));
                line4.push(JSON.stringify(sensor2_merged));
                line4.push(JSON.stringify(sensor3_merged));
                line4.push(JSON.stringify(sensor4_merged));
                line4.push(JSON.stringify(sensor5_merged));

                option4_Records.push(line4);

                option5_Records.push(line5);
            }

        try {
            await option1.writeRecords(option1_Records);
            await option2.writeRecords(option2_Records);
            await option3.writeRecords(option3_Records);
            await option4.writeRecords(option4_Records);
            await option5.writeRecords(option5_Records);
            /*await option3_5.writeRecords(option3_5_Records);*/

            console.log('Finished day: ' + (day + 1));
            option1_Records = [];
            option2_Records = [];
            option3_Records = [];
            option4_Records = [];
            option5_Records = [];
            option3_5_Records = [];
        } catch (error) {
            console.log(error);
        }
    }
})();