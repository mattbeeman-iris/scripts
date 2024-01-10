const duckdb = require('duckdb');
const AWS = require('aws-sdk');
const dynamoDb = new AWS.DynamoDB.DocumentClient({
    region: 'us-east-1',
});

const db = new duckdb.Database('./iris-alerts.db');
const conn = db.connect();

conn.all(`CREATE TABLE IF NOT EXISTS iris_alerts_olap (
    alertKey VARCHAR(50) NOT NULL,
    idCustomer INT DEFAULT NULL,
    customerKey VARCHAR(50) DEFAULT NULL,
    clientKey VARCHAR(50) DEFAULT NULL,
    monitoringType VARCHAR(50) DEFAULT NULL,
    dateAlertCreated DATETIME DEFAULT NULL,
    status VARCHAR(50) DEFAULT NULL,
    overallStatus VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (alertKey)
);`, function(err, res) {
    if(err) {
        console.log(err);
        throw new Error('Error creating table');
    } else {
        console.log('Create table response: ', JSON.stringify(res));
        return res;
    }
});

const insertAlerts = async (items) => {
    const stmt = conn.prepare('INSERT INTO iris_alerts_olap (...) VALUES (?, ?, ?, ?, ?, ?, ?, ?);');
    try {
        for (const item of items) {
            console.log('Inserting item: ', JSON.stringify([item.sk, item.idCustomer, item.customerKey, item.clientKey, item.monitoringType, item.dateAlertCreated, item.status, item.overallStatus]));
            const res = await stmt.run([item.sk, item.idCustomer, item.customerKey, item.clientKey, item.monitoringType, item.dateAlertCreated, item.status, item.overallStatus]);
            console.log('Insert result: ', JSON.stringify(res));
        }
        stmt.finalize();
    } catch (err) {
        console.log(err);
    } finally {
        conn.close();
        db.close();
    }
}

const scanIrisAlerts = async () => {
    const params = {
        TableName: 'iris-alerts',
        ProjectionExpression: '#alertKey, #idCustomer, #customerKey, #dateAlertCreated, #monitoringType, #status, #overallStatus',
        ExpressionAttributeNames: {
            '#alertKey': 'sk',
            '#idCustomer': 'idCustomer',
            '#customerKey': 'customerKey',
            '#dateAlertCreated': 'dateAlertCreated',
            '#monitoringType': 'monitoringType',
            '#status': 'status',
            '#overallStatus': 'overallStatus',
        }
        // ExclusiveStartKey: lastEvaluatedKey,
    };

    console.log('Scanning...')
    const result = await dynamoDb.scan(params).promise();

    const { LastEvaluatedKey: lastEvaluatedKey, Items: items } = result;

    
    console.log('Items: ', JSON.stringify(items));

    console.log('Inserting alerts...')
    const insertAlertsResult = await insertAlerts(items);
    console.log('Insert alerts result: ', JSON.stringify(insertAlertsResult));

    console.log('LastEvaluatedKey: ', JSON.stringify(lastEvaluatedKey));
    
    return items;
};


const queryAlerts = async () => {
    conn.all('SELECT * FROM iris_alerts_olap', function(err, res) {
        if(err) {
            console.log(err);
            throw new Error('Error querying data');
        } else {
            console.log('Query result: ', JSON.stringify(res));
            return res
        }
    });
};





async function main() {
    try {
        console.log('Scanning data...')
        const items = await scanIrisAlerts();

        console.log('Inserting alerts...')
        const insertAlertsResult = await insertAlerts(items);

    } catch (err) {
        console.log(err);
    } finally {
        conn.close();
        db.close();
    }
}

main();


