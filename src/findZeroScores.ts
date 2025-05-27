import { ethers } from "ethers";
import * as fs from "fs";
import * as readline from "readline";
import * as dotenv from "dotenv";
// @ts-ignore
const createObjectCsvWriter: any = require("csv-writer").createObjectCsvWriter;

dotenv.config();

const INPUT_FILE = "inputs/addresses.csv";
const OUTPUT_FILE = "outputs/zero-scores.csv";

const { RPC_URL, RESOLVER_CONTRACT_ADDRESS, COMMUNITY_ID } = process.env;

if (!RPC_URL || !RESOLVER_CONTRACT_ADDRESS || !COMMUNITY_ID) {
  throw new Error("Missing required environment variables. Check your .env file.");
}

const ABI = [
  {
    inputs: [
      { internalType: "uint32", name: "", type: "uint32" },
      { internalType: "address", name: "", type: "address" },
    ],
    name: "communityScores",
    outputs: [
      { internalType: "uint32", name: "score", type: "uint32" },
      { internalType: "uint64", name: "time", type: "uint64" },
      { internalType: "uint64", name: "expirationTime", type: "uint64" },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [{ internalType: "address", name: "", type: "address" }],
    name: "scores",
    outputs: [
      { internalType: "uint32", name: "score", type: "uint32" },
      { internalType: "uint64", name: "time", type: "uint64" },
      { internalType: "uint64", name: "expirationTime", type: "uint64" },
    ],
    stateMutability: "view",
    type: "function",
  },
];

const getAddresses = async (filePath: string): Promise<string[]> => {
  const addresses: string[] = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity,
  });
  for await (const line of rl) {
    const addr = line.trim();
    if (addr) addresses.push(addr);
  }
  return addresses;
};

const getZeroScoreAddresses = async (
  addresses: string[],
  contract: ethers.Contract,
  communityId: number
): Promise<string[]> => {
  const now = Math.floor(Date.now() / 1000);
  const batchSize = 10;
  let processed = 0;
  const zeroScoreAddresses: string[] = [];

  for (let i = 0; i < addresses.length; i += batchSize) {
    const batch = addresses.slice(i, i + batchSize);
    const results = await Promise.all(
      batch.map(async (address) => {
        try {
          let score, expirationTime;
          if (communityId === 335) {
            ({ score, expirationTime } = await contract.scores(address));
          } else {
            ({ score, expirationTime } = await contract.communityScores(communityId, address));
          }
          if (Number(expirationTime) > now && Number(score) === 0) {
            return address;
          }
        } catch (e) {
          console.log(`Error processing ${address}: ${e}`);
        }
        return null;
      })
    );
    zeroScoreAddresses.push(...results.filter((addr): addr is string => !!addr));
    processed += batch.length;
    if (processed % 100 === 0 || processed >= addresses.length) {
      console.log(`Processed ${processed} addresses... (${zeroScoreAddresses.length} zero score addresses found)`);
    }
  }
  return zeroScoreAddresses;
};

const writeCsv = async (addresses: string[], outputFile: string) => {
  const csvWriter = createObjectCsvWriter({
    path: outputFile,
    header: [{ id: "address", title: "address" }],
  });
  await csvWriter.writeRecords(addresses.map((address) => ({ address })));
};

const main = async () => {
  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const contract = new ethers.Contract(RESOLVER_CONTRACT_ADDRESS, ABI, provider);
  const addresses = await getAddresses(INPUT_FILE);
  const zeroScoreAddresses = await getZeroScoreAddresses(addresses, contract, Number(COMMUNITY_ID));
  await writeCsv(zeroScoreAddresses, OUTPUT_FILE);
  console.log(
    `Found ${zeroScoreAddresses.length} addresses with score 0 and unexpired. Output written to ${OUTPUT_FILE}`
  );
};

main().catch(console.error);
