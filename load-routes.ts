import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import {Client, fql, Query} from 'fauna'; // Fauna client for FQL v10

interface Route {
    airline: string | Query<any> | null;
    airlineCode: string | null;
    airlineId: string | null;
    sourceAirport: string | Query<any> | null;
    sourceAirportCode: string | null;
    sourceAirportId: string | null;
    destinationAirport: string | Query<any> |null;
    destinationAirportCode: string | null;
    destinationAirportId: string | null;
    codeshare: boolean | null;
    stops: number | null;
    equipment: string | null;
}

// Fauna client (replace with your secret key)
//const client = new Client({ secret: env.FAUNA_SECRET });
const client = new Client();

const csvFilePath = path.resolve(__dirname, 'data', 'routes.dat');
const errorLogPath = path.resolve(__dirname, 'errors.log'); // Store logs in ./data/errors.log

// Ensure `data` directory exists
if (!fs.existsSync(path.dirname(errorLogPath))) {
    fs.mkdirSync(path.dirname(errorLogPath), { recursive: true });
}

// Function to log errors
const logError = (error: string): void => {
    const timestamp = new Date().toISOString();
    fs.appendFile(errorLogPath, `[${timestamp}] ${error}\n`, (err) => {
        if (err) console.error('Failed to write to error log:', err);
    });
};

// Function to sanitize CSV values
const sanitizeValue = (key: string, value: string): string | number | boolean | null => {
    const trimmedValue = value.trim();

    if (trimmedValue === '\\N' || trimmedValue === '') return null;

    if (key === 'stops') {
        const parsed = parseInt(trimmedValue, 10);
        return isNaN(parsed) ? null : parsed;
    }

    if (key === 'codeshare') return trimmedValue.toUpperCase() === 'Y' ? true : null;

    return trimmedValue;
};

// 🔹 Function to modify route data before batching
const preprocessRoute = (route: Route): Route => {
    // Example transformations:
    if (route.airlineId != null) {
        route.airlineCode = route.airline;
        route.airline = fql`Airline.byId(${route.airlineId})`;
        route.airlineId = null;
    }

    if (route.sourceAirportId != null) {
        route.sourceAirportCode = route.sourceAirport;
        route.sourceAirport = fql`Airports.byId(${route.sourceAirportId})`;
        route.sourceAirportId = null;
    }

    if (route.destinationAirportId != null) {
        route.destinationAirportCode = route.destinationAirport;
        route.destinationAirport = fql`Airports.byId(${route.destinationAirportId})`;
        route.destinationAirportId = null;
    }

    return route;
};

// Function to write a batch of JSON documents to Fauna
const writeBatchToFauna = async (batch: Route[]): Promise<void> => {
    try {
        console.log(`Writing batch of ${batch.length} routes to Fauna...`);

        //console.log("writing the batch ", batch);

        const response = await client.query(fql`
            let routes = ${batch}
            routes.forEach(doc => Route.create({ doc }))`);
        console.log("Response from Fauna: ", response);

        console.log(`Successfully wrote batch of ${batch.length} routes.`);
    } catch (err) {
        logError(`Failed to write batch to Fauna: ${err.message || err}`);
    }
};

// Function to process all routes in batches of 10, with a 500ms pause between batches
const processRoutesInBatches = async (routes: Route[]): Promise<void> => {
    const batchSize = 10;
    let batch: Route[] = [];

    for (const route of routes) {
        batch.push(route);

        // When batch reaches 10, send it to Fauna and clear the batch
        if (batch.length === batchSize) {
            await writeBatchToFauna(batch);
            batch = []; // Reset batch

            console.log(`Pausing for 500ms before processing next batch...`);
            await new Promise(resolve => setTimeout(resolve, 10)); // Wait before sending the next batch
        }
    }

    // Process any remaining routes in the last batch
    if (batch.length > 0) {
        await writeBatchToFauna(batch);
    }

    console.log('All route batches processed successfully.');
};

// Read the CSV file and process routes in batches
const readCsvFile = async (filePath: string): Promise<void> => {
    try {
        const routes: Route[] = [];

        fs.createReadStream(filePath)
            .pipe(
                csvParser({
                    headers: [
                        'airline',
                        'airlineId',
                        'sourceAirport',
                        'sourceAirportId',
                        'destinationAirport',
                        'destinationAirportId',
                        'codeshare',
                        'stops',
                        'equipment'
                    ],
                    skipLines: 0,
                    mapValues: ({ header, value }) => sanitizeValue(header, value), // Apply sanitization
                })
            )
            .on('data', (data: Route) => {
                try {
                    const finalData = Object.fromEntries(
                        Object.entries(data).filter(([_, v]) => v !== null)
                    );

                    // 🔹 Modify route before adding to batch
                    const modifiedRoute = preprocessRoute(finalData as Route);

                    routes.push(modifiedRoute);
                } catch (err) {
                    logError(`Error processing row: ${JSON.stringify(data)} - ${err.message || err}`);
                }
            })
            .on('end', async () => {
                console.log(`Finished reading CSV file. Processing ${routes.length} routes in batches...`);
                await processRoutesInBatches(routes);
                console.log('Finished processing the CSV file and writing all batches to Fauna.');
            })
            .on('error', (err) => {
                logError(`Error reading the CSV file: ${err.message || err}`);
            });
    } catch (err) {
        logError(`General error: ${err.message || err}`);
    }
};

// Start processing
readCsvFile(csvFilePath);