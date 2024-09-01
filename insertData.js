const config = require(`./config.json`);
const path = require('path');
const { Client } = require('pg');
const Promise = require('bluebird');
const moment = require('moment-timezone');
const fs = require('fs');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');

const dataFile = 'upload.csv';
const timezone = ''; //define tz

const csvWriter = createObjectCsvWriter({
    path: 'uploadLogs.log',
    header: [
        { id: 'timestamp', title: 'Timestamp' },
        { id: 'level', title: 'Level' },
        { id: 'reference_number', title: 'Reference_Number' },
        { id: 'message', title: 'Message' },
        { id: 'details', title: 'Details' },
    ],
    append: true,
});

const logToCsv = async (level, reference_number, message, details = '') => {
    await csvWriter.writeRecords([
        {
            timestamp: moment().format('YYYY-MM-DD HH:mm:ss'),
            level,
            reference_number,
            message,
            details: typeof details === 'string' ? details : JSON.stringify(details),
        },
    ]);
};

const sleep = async (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
};

const initializePg = async (pgClient) => {
    try {
        await pgClient.connect();
    } catch (err) {
        await logToCsv('error', null, 'Database connection failed', err.message);
        await sleep(5000);
        await pgClient.connect();
    }
};

const processCsvData = async (filePath) => {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', reject);
    });
};

function formatDate(dateStr) {
    // Define possible formats
    const formats = [
        'YYYY-MM-DD HH:mm:ss',
        'YYYY-MM-DD HH:mm',
        'DD/MM/YYYY HH:mm:ss',
        'DD/MM/YYYY HH:mm'
    ];
    if (typeof dateStr !== 'string') {
        throw new Error('Date must be a string');
    }

    let parsedDate;
    for (const format of formats) {
        parsedDate = moment(dateStr, format, true);
        if (parsedDate.isValid()) {
            return parsedDate.format('YYYY-MM-DD HH:mm:ss');
        }
    }
    throw new Error('Date format not recognized');
}

const escapeSingleQuotes = (str) => {
    if (str === null || str === undefined) {
        return 'NULL';
    }
    return `'${str.replace(/'/g, "")}'`;
};

const constructQuery = (insertPart, values) => {
    const values_ = values.map(value => {

        if (value === true || value === false) {
            return escapeSingleQuotes(value);
        } else if (value === null) {
            return 'NULL';
        } else {
            return escapeSingleQuotes(value);
        }
    });
    const query = 'INSERT INTO consignmentevent(' + insertPart.join(',') + ')' + ' VALUES (' + values_.join(',') + ');';
    return query;
};
const insertEvents = async (pgClient, row, insertPart, valuesPart, event, type) => {
    if (!row || !row[event] || !type) return;
    const eventTime = moment.tz(formatDate(row[event]), timezone).utc().format('YYYY-MM-DD HH:mm:ss');
    insertPart.push('type');
    valuesPart.push(type);
    insertPart.push('event_time');
    valuesPart.push(eventTime);
    const query = constructQuery(insertPart, valuesPart);
    const queryResult = await pgClient.query(query);
    insertPart.pop();
    insertPart.pop();
    valuesPart.pop();
    valuesPart.pop();
    if (queryResult.rowCount !== 1) {
        throw new Error(`insert failed for ${type}`);
    }
};
const scheduler = async () => {
    try {
        const csvData = await processCsvData(path.join(__dirname, dataFile));

        const {
            organisationId,
            user,
            password,
            database,
            port,
            host
        } = config;

        const pgClient = new Client({
            user,
            password,
            database,
            port,
            host
        });

        pgClient.on('error', async (err) => {
            await logToCsv('error', null, 'PostgreSQL client error', err.message);
            pgClient.end();
        });
        pgClient.on('end', e => console.log('PG CONNECTION ENDED'));
        await initializePg(pgClient);

        await Promise.map(csvData, async (row) => {
            try {
                const ref = row['Reference Number'];
                if (!ref) {
                    throw new Error('Reference Number not present in file');
                };
                const consignmentQuery = `SELECT id, x_id, status FROM orders WHERE reference_number = '${ref}' and org_id = '${organisationId}';`;

                const consignmentQueryResult = await pgClient.query(consignmentQuery);
                if (consignmentQueryResult.rowCount !== 1) {
                    throw new Error('consignment not found');
                }

                const consignmentId = consignmentQueryResult.rows[0].id;
                const x_id = consignmentQueryResult.rows[0].hub_id;
                const status = consignmentQueryResult.rows[0].status;

                const currDate = new Date().toISOString();
                //const extraDetails = JSON.stringify({ added_by_custom_script: true });
                const insertPart = ['id', 'status', 'x_id'];
                const valuesPart = [_id, status, x_id];

                await insertEvents(pgClient, row, insertPart, valuesPart, 'Event1 Time', 'event1');
                await insertEvents(pgClient, row, insertPart, valuesPart, 'Event2 Time', 'event2');

                const isRto = row['Current Status'] === 'Returned to Sender' ? true : false;
                const dateFields = [
                    'Event1 Time',
                    'Event2 Time'
                ];
                const times = [];
                dateFields.forEach(field => {
                    if (row[field]) {
                        const formattedTime = moment.tz(formatDate(row[field]), tz).utc().format('YYYY-MM-DD HH:mm:ss'); //converting given timezone to UTC (db's timezone)
                        times.push(formattedTime);
                    }
                });

                cosnt updateQuery = `UPDATE orders SET is_rto = '${isRto}' WHERE id = '${consignmentId}';`;
                await pgClient.query(updateQuery);

            } catch (err) {
                await logToCsv('error', row['Reference Number'], err.message);
            }
        }, { concurrency: config.CONCURRENCY });
        pgClient.end();
        await logToCsv('info', null, 'Scheduler completed', '**********SUCCESS*************');
    } catch (err) {
        await logToCsv('error', null, 'Scheduler encountered an ERROR', err.message);
    }
};

scheduler();
