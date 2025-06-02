import { parse } from 'csv-parse';
import { createReadStream, writeFileSync } from 'fs';
import pg from 'pg';

const BATCH_SIZE = 1000;

const readIdsFromCsv = async (filePath: string): Promise<number[]> => {
  const ids: number[] = [];
  
  return new Promise((resolve, reject) => {
    const parser = parse({
      columns: true,
      skip_empty_lines: true
    });
    
    parser.on('readable', function() {
      let record;
      while ((record = parser.read()) !== null) {
        if (record.id) {
          ids.push(parseInt(record.id));
        }
      }
    });
    
    parser.on('error', reject);
    parser.on('end', () => resolve(ids));
    
    createReadStream(filePath).pipe(parser);
  });
};

const queryStampsForIds = async (client: pg.Client, ids: number[]): Promise<any[]> => {
  const query = `
    SELECT id, created_at, stamp 
    FROM ceramic_cache_ceramiccache 
    WHERE id = ANY($1::bigint[])
    ORDER BY created_at ASC
  `;
  
  const result = await client.query(query, [ids]);
  return result.rows;
};

const extractV1Nullifier = (stamp: any): string | null => {
  try {
    const stampData = typeof stamp === 'string' ? JSON.parse(stamp) : stamp;
    
    if (stampData?.credentialSubject?.nullifiers) {
      const nullifiers = stampData.credentialSubject.nullifiers;
      const v1Nullifier = nullifiers.find((n: string) => n.startsWith('v1:'));
      return v1Nullifier || null;
    }
  } catch (e) {
    return null;
  }
  return null;
};

async function main() {
  const connectionString = process.env.POSTGRES_CONNECTION_STRING;
  if (!connectionString) {
    console.error('POSTGRES_CONNECTION_STRING environment variable is required');
    process.exit(1);
  }

  const inputFile = process.argv[2] || './unique_ids.csv';
  console.log(`Reading IDs from ${inputFile}`);
  const ids = await readIdsFromCsv(inputFile);
  console.log(`Found ${ids.length} IDs to process`);

  const client = new pg.Client({
    connectionString,
    ssl: {
      rejectUnauthorized: false
    }
  });
  await client.connect();

  try {
    const allRecords: any[] = [];
    const batches = Math.ceil(ids.length / BATCH_SIZE);
    
    for (let i = 0; i < batches; i++) {
      const batchIds = ids.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);
      console.log(`Processing batch ${i + 1}/${batches} (${batchIds.length} IDs)`);
      
      const batchRecords = await queryStampsForIds(client, batchIds);
      allRecords.push(...batchRecords);
    }
    
    console.log(`Retrieved ${allRecords.length} records`);
    
    // Extract nullifiers and prepare data
    const processedRecords = allRecords.map(record => ({
      id: record.id,
      created_at: new Date(record.created_at).toISOString(),
      v1_nullifier: extractV1Nullifier(record.stamp)
    })).filter(r => r.v1_nullifier !== null);
    
    console.log(`Found ${processedRecords.length} records with v1 nullifiers`);
    
    // Write to CSV
    const csvRows = ['id,created_at,v1_nullifier'];
    processedRecords.forEach(record => {
      csvRows.push(`${record.id},${record.created_at},${record.v1_nullifier}`);
    });
    
    const outputFile = './records_with_nullifiers.csv';
    writeFileSync(outputFile, csvRows.join('\n'));
    console.log(`Results written to ${outputFile}`);
    
    // Analyze for originals vs copies
    const nullifierFirstUse = new Map<string, { id: number, created_at: string }>();
    
    // Sort by created_at to find first use of each nullifier
    const sortedRecords = [...processedRecords].sort((a, b) => 
      new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
    );
    
    // Find first occurrence of each nullifier
    sortedRecords.forEach(record => {
      if (!nullifierFirstUse.has(record.v1_nullifier!)) {
        nullifierFirstUse.set(record.v1_nullifier!, {
          id: record.id,
          created_at: record.created_at
        });
      }
    });
    
    // Create pivot table friendly CSV
    const pivotRows = ['id,created_at,v1_nullifier,is_original,stamp_type'];
    processedRecords.forEach(record => {
      const firstUse = nullifierFirstUse.get(record.v1_nullifier!);
      const isOriginal = firstUse!.id === record.id;
      const stampType = isOriginal ? 'original' : 'copy';
      
      pivotRows.push(`${record.id},${record.created_at},${record.v1_nullifier},${isOriginal},${stampType}`);
    });
    
    const pivotFile = './nullifier_analysis_pivot.csv';
    writeFileSync(pivotFile, pivotRows.join('\n'));
    console.log(`Pivot table data written to ${pivotFile}`);
    
    // Quick stats
    const nullifierCounts = new Map<string, number>();
    processedRecords.forEach(record => {
      const count = nullifierCounts.get(record.v1_nullifier!) || 0;
      nullifierCounts.set(record.v1_nullifier!, count + 1);
    });
    
    const duplicates = Array.from(nullifierCounts.entries()).filter(([_, count]) => count > 1);
    console.log(`\nUnique v1 nullifiers: ${nullifierCounts.size}`);
    console.log(`Nullifiers used multiple times: ${duplicates.length}`);
    
    // Count originals vs copies
    const originals = processedRecords.filter(r => {
      const firstUse = nullifierFirstUse.get(r.v1_nullifier!);
      return firstUse!.id === r.id;
    });
    console.log(`Original stamps: ${originals.length}`);
    console.log(`Copy stamps: ${processedRecords.length - originals.length}`);
    
  } finally {
    await client.end();
  }
}

main().catch(console.error);