import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import {Client, FaunaError, fql, ServiceError} from "fauna";

interface Country {
    name: string | null;
    iso_code: string | null;
    dafif_code: string | null;
}

const csvFilePath = path.resolve(__dirname, 'data', 'countries.dat');
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

const sanitizeValue = (value: string): string | null => {
    const trimmedValue = value.trim();

    // Handle special case for null values
    if (trimmedValue === '\\N' || trimmedValue === '') {
        return null;
    }

    return trimmedValue;
};

const writeToFauna = async (data: Country): Promise<void> => {
    const client = new Client({ secret: "fnAF2aKyTiAARMXqxzfczNRowrVYAZKYYVIOxFgv" });

    try {
        const getData = await client.query(
            fql`Country.create(${data})`
        );

        console.log("data inserted");
    } catch (error) {
        if (error instanceof FaunaError) {
            if (error instanceof ServiceError) {
                console.error(error.queryInfo?.summary);
                logError(`Failed to write to Fauna: ${error.queryInfo?.summary || error}`);
            } else {
                // Otherwise, return a generic error response for
                // Fauna errors.
                console.log("data failed to insert ", error);
                logError(`Failed to write to FaunaDB: ${error}`);
            }
        }
        console.log("data failed to insert ", error);
    }

    console.log(`Writing to FaunaDB: ${JSON.stringify(data, null, 2)}`);
};

const readCsvFile = async (filePath: string): Promise<void> => {
    try {
        const promises: Promise<void>[] = [];

        fs.createReadStream(filePath)
            .pipe(
                csvParser({
                    headers: ['name', 'iso_code', 'dafif_code'],
                    skipLines: 0,
                    mapValues: ({ value }) => sanitizeValue(value), // Apply sanitization
                })
            )
            .on('data', async (data: Country) => {
                try {
                    // Remove keys with null values from the JSON
                    const finalData = Object.fromEntries(
                        Object.entries(data).filter(([_, v]) => v !== null)
                    );

                    // Push the async write to the promises array
                    promises.push(writeToFauna(finalData as Country));
                } catch (err) {
                    logError(`Error processing row: ${JSON.stringify(data)} - ${err.message || err}`);
                }
            })
            .on('end', async () => {
                try {
                    // Await all asynchronous writes
                    await Promise.all(promises);
                    console.log('Finished processing the CSV file and writing to FaunaDB.');
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