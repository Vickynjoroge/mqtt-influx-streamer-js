const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const mqtt = require('mqtt');
const redis = require('redis')
require('dotenv').config();

/**
 * Streamer - A class to handle data reception and write to InfluxDB
 */
class Streamer {
    /**
     * @constructor - Initialize MQTT connection and setup stream
     * @param {object} mqttConfig - MQTT configuration object
     * @param {object} influxConfig - InfluxDB configuration object
     * @param {string} measurement - Measurement name in InfluxDB
     * @param {Array<string>} [tags] - Optional array of tag names
     * @param {Array<string>} [fields] - Optional array of field names
     */
    constructor(mqttConfig, redisConfig, measurement = 'default_measurement', tags = [], fields = []) {
        this.mqttConfig = mqttConfig;
        this.measurement = measurement;
        this.tags = tags;
        this.fields = fields;
    // MQTT Client set up
        this.client = mqtt.connect(mqttConfig.url, {
            username: mqttConfig.username,
            password: mqttConfig.password,
        });

        this.client.on('connect', () => {
            console.log('Connected to MQTT broker');
            this.client.subscribe(mqttConfig.topic, (err) => {
                if (!err) {
                    console.log('Subscribed to topic');
                } else {
                    console.error('Subscription error:', err);
                }
            });
        });

        this.client.on('message', this.handleMessage.bind(this));
        this.client.on('error', (err) => {
            console.error('Connection error:', err);
        });

    // Redis setup
    this.redisClient = redis.createClient({
        host: redisConfig.host,
        port: redisConfig.port,
        password: redisConfig.password,
    });
    this.redisClient.on('connect', () => {
        console.log('Connected to Redis');
    });

    this.redisClient.on('error', (err) => {
        console.error('Redis error:', err);        
    })
    }

    /**
     * @handleMessage - Shows received payload and streams to InfluxDB
     * @param {string} topic - MQTT topic subscribed to
     * @param {Buffer} message - The received payload
     */
    handleMessage(topic, message) {
        console.log(`Received message on ${topic}: ${message.toString()}`);

        // Storing to Redis
        const key = `${topic}_${Date.now()}`
        this.redisClient.set(key, message.toString(), (err, reply) => {
            if (err) {
                console.error("Error storing data in Redis", err);                
            } else {
                console.log(`Data stored in redis with key "${key}": ${reply}`);                
            }
        })
    }

}


const my_mqtt_config = {
    url: process.env.BROKER_URL,
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    topic: process.env.MQTT_TOPIC
};

// Configuration objects using environment variables
const redis_config = {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
}

const measurement = 'some_measurement';
const tags = ['tag1', 'tag2', 'tag3'];
const fields = ['field1', 'field2', 'field3'];

const streamer = new Streamer(my_mqtt_config, redis_config,measurement, tags, fields);

