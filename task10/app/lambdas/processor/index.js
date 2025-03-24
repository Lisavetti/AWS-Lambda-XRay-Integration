const AWS = require("aws-sdk");
const axios = require("axios");
const { v4: uuidv4 } = require("uuid");

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const TABLE_NAME = process.env.target_table || "Weather";

async function fetchWeather() {
    const url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

    try {
        const response = await axios.get(url);
        console.log("Fetched weather data:", JSON.stringify(response.data, null, 2));
        return response.data;
    } catch (error) {
        console.error("Error fetching weather data:", error);
        throw new Error("Failed to fetch weather data");
    }
}

exports.handler = async (event) => {
    try {
        console.log("Received event:", JSON.stringify(event, null, 2));

        const weatherData = await fetchWeather();

        const item = {
            id: uuidv4(),
            forecast: {
                elevation: weatherData.elevation,
                generationtime_ms: weatherData.generationtime_ms,
                hourly: {
                    temperature_2m: weatherData.hourly.temperature_2m,
                    time: weatherData.hourly.time,
                },
                hourly_units: {
                    temperature_2m: weatherData.hourly_units.temperature_2m,
                    time: weatherData.hourly_units.time,
                },
                latitude: weatherData.latitude,
                longitude: weatherData.longitude,
                timezone: weatherData.timezone,
                timezone_abbreviation: weatherData.timezone_abbreviation,
                utc_offset_seconds: weatherData.utc_offset_seconds,
            },
        };

        console.log("Saving item to DynamoDB:", JSON.stringify(item, null, 2));

        await dynamoDB.put({
            TableName: TABLE_NAME,
            Item: item
        }).promise().then(() => {
            console.log("Successfully inserted item into DynamoDB");
        }).catch(err => {
            console.error("DynamoDB put error:", err);
            throw new Error("Failed to store data in DynamoDB");
        });

        return {
            statusCode: 200,
            body: JSON.stringify({ message: "Weather data stored successfully!" }),
            headers: { "Content-Type": "application/json" }
        };

    } catch (error) {
        console.error("Error processing request:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal Server Error", error: error.message }),
            headers: { "Content-Type": "application/json" }
        };
    }
};