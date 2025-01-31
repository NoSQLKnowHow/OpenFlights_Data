import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import {Client, fql, Query} from 'fauna'; // Fauna client for FQL v10

interface Airport {
  id: number | null;
  name: string | null;
  citymain: string | null;
  country: any | Query | null;
  iata: string | null;
  icao: string | null;
  latitude: string | null;
  longitude: string | null;
  location?: {
    latitude: string | null;
    longitude: string | null;
  };
  altitude: number | null;
  timezone: number | null;
  dst: string | null;
  tzdatabasetimezone: string | null;
  type: string | null;
  source: string | null;
}

// Fauna client (gets the secret key from the FAUNA_SECRET environment variable)
const client = new Client();

const csvFilePath = path.resolve(__dirname, 'data', 'airports.dat');
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
const sanitizeValue = (key: string, value: string): string | number | null => {
  const trimmedValue = value.trim();

  if (trimmedValue === '\\N' || trimmedValue === '') return null;

  if (key === 'id' || key === 'altitude' || key === 'timezone') {
    const parsed = parseFloat(trimmedValue);
    return isNaN(parsed) ? null : parsed;
  }

  return trimmedValue;
};

// 🔹 Function to modify airport data before batching
const preprocessAirport = (airport: Airport): Airport => {
  // Convert the `country` field into an FQL query expression
  if (airport.country) {
    airport.country = fql`Country.byName(${airport.country}).first()`;
  }

  // Nest latitude and longitude under `location` and delete the existing fields not nested.
  if (airport.latitude && airport.longitude) {
    airport.location = {
      latitude: airport.latitude,
      longitude: airport.longitude,
    };
    airport.latitude = null;
    airport.longitude = null;
  }

  return airport;
};

// Function to write a batch of airports to Fauna
const writeBatchToFauna = async (batch: Airport[]): Promise<void> => {
  try {
    console.log(`Writing batch of ${batch.length} airports to Fauna...`);

    // Convert batch to an array of JSON documents
    const jsonBatch = batch.map(airport => ({
      data: airport
    }));

    // Send batch write to Fauna (assuming a batch UDF `create_airports_batch`)
    const response = await client.query(fql`
            let airports = ${batch}
            airports.forEach(doc => Airport.create({ doc }))`);
    console.log("Response from Fauna: ", response);

    console.log(`Successfully wrote batch of ${batch.length} airports.`);
  } catch (err) {
    logError(`Failed to write batch to Fauna: ${err.message || err}`);
  }
};

// Function to process all airports in batches of 10, with a 10ms pause between batches
const processAirportsInBatches = async (airports: Airport[]): Promise<void> => {
  const batchSize = 10;
  let batch: Airport[] = [];

  for (const airport of airports) {
    batch.push(airport);

    // When batch reaches 10, send it to Fauna and clear the batch
    if (batch.length === batchSize) {
      await writeBatchToFauna(batch);
      batch = []; // Reset batch

      console.log(`Pausing for 10ms before processing next batch...`);
      await new Promise(resolve => setTimeout(resolve, 10)); // Wait before sending the next batch
    }
  }

  // Process any remaining airports in the last batch
  if (batch.length > 0) {
    await writeBatchToFauna(batch);
  }

  console.log('All airport batches processed successfully.');
};

// Read the CSV file and process airports in batches
const readCsvFile = async (filePath: string): Promise<void> => {
  try {
    const airports: Airport[] = [];

    fs.createReadStream(filePath)
        .pipe(
            csvParser({
              headers: [
                'id', 'name', 'citymain', 'country', 'iata', 'icao',
                'latitude', 'longitude', 'altitude', 'timezone', 'dst',
                'tzdatabasetimezone', 'type', 'source'
              ],
              skipLines: 0,
              mapValues: ({ header, value }) => sanitizeValue(header, value), // Apply sanitization
            })
        )
        .on('data', (data: Airport) => {
          try {
            const finalData = Object.fromEntries(
                Object.entries(data).filter(([_, v]) => v !== null)
            );

            // 🔹 Modify airport before adding to batch
            const modifiedAirport = preprocessAirport(finalData as Airport);

            airports.push(modifiedAirport);
          } catch (err) {
            logError(`Error processing row: ${JSON.stringify(data)} - ${err.message || err}`);
          }
        })
        .on('end', async () => {
          console.log(`Finished reading CSV file. Processing ${airports.length} airports in batches...`);
          await processAirportsInBatches(airports);
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