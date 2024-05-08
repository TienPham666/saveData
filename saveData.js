const { Sequelize, DataTypes } = require('sequelize');
const moment = require('moment');
const mqtt = require('mqtt');
const sequelize = new Sequelize('innovation', 'root', 'hieu27112001', {
    host: 'innovation.cxe62qeykxwn.ap-southeast-2.rds.amazonaws.com',
    dialect: 'mysql'
});

const Sensors = sequelize.define('Sensor', {
    sensors_id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true
    },
    stations_id: {
        type: DataTypes.STRING
    },
    sensors_name: {
        type: DataTypes.STRING
    },
    sensors_value: {
        type: DataTypes.FLOAT
    },
    sensors_datetime: {
        type: DataTypes.DATE
    },
    sensors_flag: {
        type: DataTypes.BOOLEAN
    }
}, {
    tableName: 'sensors', // Tên bảng là 'sensors'
    timestamps: false // Vô hiệu hóa createdAt và updatedAt
});

sequelize.sync()
    .then(() => {
        console.log('Mô hình đã được đồng bộ hóa với cơ sở dữ liệu');
    })
    .catch(err => {
        console.error('Không thể đồng bộ hóa mô hình với cơ sở dữ liệu:', err);
    });

const brokerUrl = 'mqtt://mqttserver.tk';
const brokerPort = 1883;
const brokerUsername = 'innovation';
const brokerPassword = 'Innovation_RgPQAZoA5N';
const topic = '/innovation/airmonitoring/WSNs';
const CLIENT_ID = 'innovation';
const CLEAN_SESSION = true;

const client = mqtt.connect(brokerUrl, {
    host: brokerUrl,
    port: brokerPort,
    username: brokerUsername,
    password: brokerPassword,
    clientId: CLIENT_ID,
    clean: CLEAN_SESSION,
});

client.on('connect', function () {
    console.log('Connected to MQTT broker');
    client.subscribe(topic, function (err) {
        if (!err) {
            console.log('Subscribed to', topic);
        }
    });
});

client.on('message', async (topic, receivedMessage) => {
    const messageString = receivedMessage.toString();

    try {
        const jsonStringWithDoubleQuotes = messageString.replace(/'/g, '"');
        const messageJSON = JSON.parse(jsonStringWithDoubleQuotes);
        console.log(`Received JSON message on topic ${topic}:`, messageJSON);
        // Xử lý thông điệp JSON ở đây
        const check = await checkAndSaveData(messageJSON); // Chờ hàm này hoàn thành trước khi tiếp tục
        console.log(check)
        await saveDataToDatabase(messageJSON, check); // Chờ hàm này hoàn thành trước khi tiếp tục
    } catch (error) {
        console.error('Lỗi khi phân tích JSON:', error);
        // Xử lý lỗi ở đây
    }
});


async function checkAndSaveData(jsonData) {
    try {
        // Lấy dữ liệu cảm biến đã lưu trữ trước đó
        const storedSensorData = await getStoredSensorData(jsonData.station_id, jsonData.sensors.map(sensor => sensor.id));
        // Kiểm tra xem dữ liệu mới có giống dữ liệu đã lưu không
        let newDataDifferent = false;

        // So sánh dữ liệu mới với dữ liệu đã lưu trữ
        jsonData.sensors.forEach(sensor => {
            const storedDataForSensor = storedSensorData.find(data => data.sensors_name === sensor.id && data.stations_id === jsonData.station_id);
            if (!storedDataForSensor || storedDataForSensor.sensors_value != sensor.value) {
                newDataDifferent = true;
            }
        });

        if (newDataDifferent) {
            return true;
        } else {
            // Nếu không có cảm biến nào khác biệt, không lưu trữ và trả về false
            return false;
        }
    } catch (error) {
        console.error('Error checking and saving data:', error);
        return false;
    }
}

async function getStoredSensorData(stationsId, sensorsNames, limit = 22) {
    try {
        const results = await Sensors.findAll({
            where: {
                stations_id: stationsId,
                sensors_name: sensorsNames,
            },
            attributes: ['stations_id', 'sensors_name', 'sensors_value', 'sensors_id'],
            order: [['sensors_id', 'DESC']],
            limit: limit,
        });
        const reversedResults = results.reverse();
        return reversedResults;
    } catch (error) {
        throw error;
    }
}

async function saveDataToDatabase(jsonData, check) {
    const currentDateTime = moment().format('YYYY-MM-DD HH:mm:ss');

    try {
        const promises = jsonData.sensors.map(sensor => {
            return Sensors.create({
                stations_id: jsonData.station_id,
                sensors_name: sensor.id,
                sensors_value: sensor.value,
                sensors_datetime: currentDateTime,
                sensors_flag: check,
            });
        });

        await Promise.all(promises);
        console.log('Inserted sensor data successfully');
    } catch (error) {
        console.error('Error inserting sensor data:', error.message);
    }
}