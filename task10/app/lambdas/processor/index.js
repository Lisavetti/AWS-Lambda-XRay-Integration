const axios = require('axios');
const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');

const documentClient = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    const tbName = process.env.target_table || 'cmtr-1c7e15d3-Weather';

    const weatherApiUrl =
        'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m';

    try {
        const response = await axios.get(weatherApiUrl);
        const weatherData = response.data;

        const weatherRecord = {
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

        const params = {
            TableName: tbName,
            Item: weatherRecord,
        };

        await documentClient.put(params).promise();

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Weather data inserted into DynamoDB successfully',
            }),
        };
    } catch (error) {
        console.error(
            'Error fetching weather data or inserting into DynamoDB:',
            error,
        );

        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'Failed to insert weather data into DynamoDB',
                error: error.message,
            }),
        };
    }
};