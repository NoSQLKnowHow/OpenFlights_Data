import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import { Client, fql, FaunaError, ServiceError } from "fauna";

interface Airport {
  id: number | null;
  name: string | null;
  citymain: string | null;
  country: string | null;
  iata: string | null;
  icao: string | null;
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

export interface Env {
  FAUNA_SECRET: string;
}

const csvFilePath = path.resolve(__dirname, 'data', 'airports.dat');
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

const sanitizeValue = (key: string, value: string): string | number | null => {
  const trimmedValue = value.trim();

  // Handle special case for null values
  if (trimmedValue === '\\N') {
    return null;
  }

  // Convert id, altitude, and timezone to numbers
  if (key === 'id' || key === 'altitude' || key === 'timezone') {
    const parsed = parseFloat(trimmedValue);
    return isNaN(parsed) ? null : parsed;
  }

  // Return sanitized string
  return trimmedValue;
};

const writeToFauna = async (data: Airport): Promise<void> => {
  const client = new Client({ secret: "fnAF2aKyTiAARMXqxzfczNRowrVYAZKYYVIOxFgv" });

  try {
    // Transform the country field into an FQL query
    const { country, ...rest } = data;

    // Build the final payload with the transformed country field
    const payload = {
      ...rest,
      country: fql`Country.byName(${country}).first()`,
    };

    const getData = await client.query(
        fql`Airport.create(${payload})`
    );

    console.log("data inserted");
  } catch (error) {
    if (error instanceof FaunaError) {
      if (error instanceof ServiceError) {
        console.error(error.queryInfo?.summary);
        logError(`Failed to write to Fauna: ${error.queryInfo?.summary || error}`);
      } else {
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
              headers: [
                'id', 'name', 'citymain', 'country', 'iata', 'icao',
                'latitude', 'longitude', 'altitude', 'timezone', 'dst',
                'tzdatabasetimezone', 'type', 'source'
              ],
              skipLines: 0,
              mapValues: ({ header, value }) => sanitizeValue(header, value), // Apply sanitization per key
            })
        )
        .on('data', async (data: Airport) => {
          try {
            // Extract latitude and longitude into a location object
            const location = {
              latitude: data.latitude,
              longitude: data.longitude,
            };

            // Remove latitude and longitude from the top level
            const { latitude, longitude, ...rest } = data;

            // Add the location object only if both latitude and longitude are not null
            const sanitizedData: Airport = {
              ...rest,
              location: location.latitude && location.longitude ? location : undefined,
            };

            // Remove keys with null values from the JSON
            const finalData = Object.fromEntries(
                Object.entries(sanitizedData).filter(([_, v]) => v !== null)
            );

            // Push the async write to the promises array
            promises.push(writeToFauna(finalData as Airport));
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