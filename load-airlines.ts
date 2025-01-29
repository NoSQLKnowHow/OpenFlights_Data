import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import { Client, query as q } from 'fauna';
import {FaunaError, fql, ServiceError} from "fauna";

interface Airline {
    id: string | null;
    name: string | null;
    alias: string | null;
    IATA: string | null;
    ICAO: string | null;
    callsign: string | null;
    country: string | null;
    active: boolean | null;
}

// Fauna client (replace with your secret key)
const client = new Client({ secret: 'YOUR_FAUNA_SECRET' });

const csvFilePath = path.resolve(__dirname, 'airlines.dat');
const errorLogPath = path.resolve(__dirname, 'errors.log');

// Function to log errors to a file
const logError = (error: string): void => {
    const timestamp = new Date().toISOString();
    fs.appendFile(errorLogPath, `[${timestamp}] ${error}\n`, (err) => {
        if (err) {
            console.error('Failed to write to error log:', err);
        }
    });
};

const sanitizeValue = (key: string, value: string): string | boolean | null => {
    const trimmedValue = value.trim();

    // Handle special case for null values
    if (trimmedValue === '\\N' || trimmedValue === '') {
        return null;
    }

    // Convert `active` field: "Y" -> true, anything else -> false
    if (key === 'active') {
        return trimmedValue.toUpperCase() === 'Y';
    }

    // Return sanitized string
    return trimmedValue;
};

const writeToFauna = async (data: Airline): Promise<void> => {
    const client = new Client({ secret: "fnAF2OjlVlAAQVx6pInNHHtafICDzDNaoWZOQ3Rh" });

    try {
        // Transform the country field into an FQL query
        const { country, ...rest } = data;

        // Build the final payload with the transformed country field
        const payload = {
            ...rest,
            country: fql`Countries.byName(${country}).first()`,
        };

        const getData = await client.query(
            fql`Airline.create(${payload})`
        );

        console.log("data inserted");
    } catch (error) {
        if (error instanceof FaunaError) {
            if (error instanceof ServiceError) {
                console.error(error.queryInfo?.summary);
                logError(`Failed to write to Fauna: ${error.queryInfo?.summary || error}`);
            } else {
                console.log("data failed to insert ", error);
                logError(`Failed to write to Fauna: ${error}`);
            }
        }
        console.log("data failed to insert ", error);
    }

    console.log(`Writing to Fauna: ${JSON.stringify(data, null, 2)}`);
};

const readCsvFile = async (filePath: string): Promise<void> => {
    try {
        const promises: Promise<void>[] = [];

        fs.createReadStream(filePath)
            .pipe(
                csvParser({
                    headers: ['id', 'name', 'alias', 'IATA', 'ICAO', 'callsign', 'country', 'active'],
                    skipLines: 0,
                    mapValues: ({ header, value }) => sanitizeValue(header, value), // Apply sanitization per key
                })
            )
            .on('data', async (data: Airline) => {
                try {
                    // Remove keys with null values from the JSON
                    const finalData = Object.fromEntries(
                        Object.entries(data).filter(([_, v]) => v !== null)
                    );

                    // Push the async write to the promises array
                    promises.push(writeToFauna(finalData as Airline));
                } catch (err) {
                    logError(`Error processing row: ${JSON.stringify(data)} - ${err.message || err}`);
                }
            })
            .on('end', async () => {
                try {
                    // Await all asynchronous writes
                    await Promise.all(promises);
                    console.log('Finished processing the CSV file and writing to Fauna.');
                } catch (err) {
                    logError(`Error awaiting promises: ${err.message || err}`);
                }
            })
            .on('error', (err) => {
                logError(`Error reading the CSV file: ${err.message || err}`);
            });
    } catch (err) {
        logError(`General error: ${err.message || err}`);
    }
};

readCsvFile(csvFilePath);