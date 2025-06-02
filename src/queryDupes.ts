import { parse } from 'csv-parse/sync';
import { readFileSync, writeFileSync } from 'fs';
import pg from 'pg';

const BATCH_SIZE = 10000; // Larger batch size to reduce DB queries

const readAndParseCsv = (filePath: string): any[] => {
  const csvContent = readFileSync(filePath, 'utf-8');
  return parse(csvContent, {
    columns: true,
    skip_empty_lines: true
  });
};

const extractUniqueIds = (records: any[]): number[] => {
  const idSet = records.reduce((acc, record) => {
    if (record.id_a) acc.add(parseInt(record.id_a));
    if (record.id_b) acc.add(parseInt(record.id_b));
    return acc;
  }, new Set<number>());
  
  return Array.from(idSet);
};

const createBatches = <T>(items: T[], batchSize: number): T[][] => {
  return Array.from(
    { length: Math.ceil(items.length / batchSize) },
    (_, i) => items.slice(i * batchSize, (i + 1) * batchSize)
  );
};

const queryBatch = async (client: pg.Client, ids: number[]): Promise<any[]> => {
  const query = `
    SELECT id, created_at 
    FROM ceramic_cache_ceramiccache 
    WHERE id = ANY($1::bigint[])
    ORDER BY id
  `;
  
  const result = await client.query(query, [ids]);
  return result.rows;
};

const formatAsCsv = (rows: any[]): string => {
  const header = 'created_at,id';
  const dataRows = rows.map(row => {
    // Convert to ISO format for consistent sorting
    const isoDate = new Date(row.created_at).toISOString();
    return `${isoDate},${row.id}`;
  });
  return [header, ...dataRows].join('\n');
};

const processSequentially = async (
  client: pg.Client, 
  ids: number[], 
  batchSize: number
): Promise<any[]> => {
  const batches = createBatches(ids, batchSize);
  console.log(`Processing ${batches.length} batches of up to ${batchSize} IDs each`);
  
  const allResults: any[] = [];
  
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Processing batch ${i + 1}/${batches.length} (${batch.length} IDs)`);
    const batchResults = await queryBatch(client, batch);
    allResults.push(...batchResults);
  }
  
  return allResults;
};

async function main() {
  const connectionString = process.env.POSTGRES_CONNECTION_STRING;
  if (!connectionString) {
    console.error('POSTGRES_CONNECTION_STRING environment variable is required');
    process.exit(1);
  }

  // Extract unique IDs from CSV
  const records = readAndParseCsv('./dupes.csv');
  const uniqueIds = extractUniqueIds(records);
  console.log(`Found ${uniqueIds.length} unique IDs to query`);

  // Connect to PostgreSQL
  const client = new pg.Client({
    connectionString,
    ssl: {
      rejectUnauthorized: false
    }
  });
  await client.connect();

  try {
    // Query in sequential batches
    const allResults = await processSequentially(client, uniqueIds, BATCH_SIZE);
    console.log(`Retrieved ${allResults.length} records from database`);

    // Write results to CSV
    const csvOutput = formatAsCsv(allResults);
    writeFileSync('./dupes_with_timestamps.csv', csvOutput);
    console.log('Results written to dupes_with_timestamps.csv');

  } finally {
    await client.end();
  }
}

main().catch(console.error);