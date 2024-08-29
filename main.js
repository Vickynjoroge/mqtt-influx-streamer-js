const mqtt = require('mqtt'); // Import MQTT library to handle MQTT connections
const amqp = require('amqplib/callback_api'); // Import RabbitMQ library for AMQP (Advanced Message Queuing Protocol)
require('dotenv').config(); // Load environment variables from a .env file

/**
 * Streamer - A class to handle data reception from MQTT and sending it to RabbitMQ
 */
class Streamer {
    /**
     * @constructor - Initialize MQTT connection and RabbitMQ setup
     * @param {object} mqttConfig - Configuration object for MQTT connection (URL, username, password, topic)
     * @param {object} rabbitConfig - Configuration object for RabbitMQ connection (URL, queue name)
     * @param {string} measurement - Measurement name for InfluxDB (not used yet in this code)
     * @param {Array<string>} [tags] - Optional array of tag names (not used yet in this code)
     * @param {Array<string>} [fields] - Optional array of field names (not used yet in this code)
     */
    constructor(mqttConfig, rabbitConfig, measurement = 'default_measurement', tags = [], fields = []) {
        this.mqttConfig = mqttConfig; // Store MQTT configuration
        this.rabbitConfig = rabbitConfig; // Store RabbitMQ configuration
        this.measurement = measurement; // Store measurement name (not used in this code)
        this.tags = tags; // Store tag names (not used in this code)
        this.fields = fields; // Store field names (not used in this code)

        // Initialize MQTT client using the provided configuration
        this.client = mqtt.connect(mqttConfig.url, {
            username: mqttConfig.username,
            password: mqttConfig.password,
        });

        // Event listener for when the MQTT client successfully connects to the broker
        this.client.on('connect', () => {
            console.log('Connected to MQTT broker');
            // Subscribe to the specified MQTT topic
            this.client.subscribe(mqttConfig.topic, (err) => {
                if (!err) {
                    console.log('Subscribed to topic');
                } else {
                    console.error('Subscription error:', err);
                }
            });
        });

        // Event listener for receiving messages on the subscribed MQTT topic
        this.client.on('message', this.handleMessage.bind(this)); // Bind handleMessage to preserve 'this' context

        // Event listener for handling MQTT connection errors
        this.client.on('error', (err) => {
            console.error('Connection error:', err);
        });

        // Initialize RabbitMQ connection
        amqp.connect(rabbitConfig.url, (error0, connection) => {
            if (error0) {
                console.error('Error connecting to RabbitMQ:', error0);
                return;
            }
            this.rabbitConnection = connection; // Store the RabbitMQ connection
            // Create a channel to communicate with RabbitMQ
            this.rabbitConnection.createChannel((error1, channel) => {
                if (error1) {
                    console.error('Error creating RabbitMQ channel:', error1);
                    return;
                }
                this.channel = channel; // Store the RabbitMQ channel
                // Ensure the queue exists before sending messages to it
                this.channel.assertQueue(rabbitConfig.queue, {
                    durable: false // Queue is not durable, meaning it won't survive server restarts
                });
                console.log('Connected to RabbitMQ');
                this.rabbitConnected = true; // Mark RabbitMQ as connected
            });
        });

        // Ensure RabbitMQ is connected before trying to use it
        this.rabbitConnected = false;
    }

    /**
     * @handleMessage - Handles the received payload from MQTT and sends it to RabbitMQ
     * @param {string} topic - The MQTT topic the message was received on
     * @param {Buffer} message - The received payload in Buffer format
     */
    async handleMessage(topic, message) {
        const data = JSON.parse(message.toString()); // Convert the message buffer to a JSON object
        console.log(`Received message on ${topic}:`, data);

        // Check if RabbitMQ is connected before sending data
        if (this.rabbitConnected) {
            try {
                // Create a unique key based on the topic and current timestamp
                const key = `${topic}_${Date.now()}`;
                // Send the data to the specified RabbitMQ queue
                this.channel.sendToQueue(this.rabbitConfig.queue, Buffer.from(JSON.stringify(data)));
                console.log(`Data sent to RabbitMQ with key "${key}"`);
            } catch (err) {
                console.error('Error sending data to RabbitMQ:', err);
            }
        } else {
            console.error('Cannot send data to RabbitMQ: RabbitMQ client is not connected');
        }

        // Placeholder for writing data to InfluxDB (not yet implemented)
        // Example code for writing to InfluxDB would go here
    }
}

// Load configuration for MQTT from environment variables
const my_mqtt_config = {
    url: process.env.BROKER_URL, // MQTT broker URL
    username: process.env.MQTT_USERNAME, // MQTT username
    password: process.env.MQTT_PASSWORD, // MQTT password
    topic: process.env.MQTT_TOPIC, // MQTT topic to subscribe to
};

// Load configuration for RabbitMQ from environment variables
const rabbit_config = {
    url: process.env.RABBITMQ_URL || 'amqp://localhost', // RabbitMQ server URL, defaults to localhost
    queue: process.env.RABBITMQ_QUEUE || 'mqtt_data_queue', // RabbitMQ queue name, defaults to 'mqtt_data_queue'
};

// Define measurement, tags, and fields (not used in this code, but could be used for InfluxDB)
const measurement = 'some_measurement';
const tags = ['site_id', 'site_group_id', 'site_device_reference'];
const fields = ['1912', '1913', '1914', '1915', '1916', '1917', '1918', '1919', '1920', '1927', '1928'];

// Instantiate the Streamer class with the loaded configurations
const streamer = new Streamer(my_mqtt_config, rabbit_config, measurement, tags, fields);
