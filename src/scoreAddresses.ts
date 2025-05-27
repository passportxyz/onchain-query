import { ethers } from "ethers";
import * as fs from "fs";
import * as readline from "readline";
import * as dotenv from "dotenv";
import axios from "axios";

dotenv.config();

const INPUT_FILE = "inputs/score_addresses.csv";
const OUTPUT_FILE = "outputs/scored_addresses.csv";

const { 
  RPC_URL, 
  PRIVATE_KEY, 
  SCORER_API_KEY, 
  SCORER_ENDPOINT, 
  COMMUNITY_ID,
  ATTESTER_CONTRACT_ADDRESS,
  SCHEMA_UID
} = process.env;

if (!RPC_URL || !PRIVATE_KEY || !SCORER_API_KEY || !SCORER_ENDPOINT || !COMMUNITY_ID || !ATTESTER_CONTRACT_ADDRESS || !SCHEMA_UID) {
  throw new Error("Missing required environment variables. Check your .env file.");
}

const SCORE_DECIMALS = 4;
const ZERO_BYTES32 = "0x0000000000000000000000000000000000000000000000000000000000000000";

const parseDecimal = (decimalStr: string): bigint => {
  // Turns e.g. 2.12345 into 2.1234 without using float math
  const truncated = decimalStr.replace(new RegExp(String.raw`(?<=\.\d{${SCORE_DECIMALS}})\d+`), "");
  return ethers.parseUnits(truncated, SCORE_DECIMALS);
};

type V2ScoreResponseData = {
  address: string;
  score: string;
  threshold: string;
  passing_score: boolean;
  last_score_timestamp: string;
  expiration_timestamp: string;
  error?: string;
  stamps: {
    [provider: string]: {
      score: string;
      dedup: boolean;
      expiration_date?: string;
    };
  };
};

// GitcoinAttester contract ABI
const ATTESTER_ABI = [
  {
    inputs: [
      {
        components: [
          {
            internalType: "bytes32",
            name: "schema",
            type: "bytes32"
          },
          {
            components: [
              {
                internalType: "address",
                name: "recipient",
                type: "address"
              },
              {
                internalType: "uint64",
                name: "expirationTime",
                type: "uint64"
              },
              {
                internalType: "bool",
                name: "revocable",
                type: "bool"
              },
              {
                internalType: "bytes32",
                name: "refUID",
                type: "bytes32"
              },
              {
                internalType: "bytes",
                name: "data",
                type: "bytes"
              },
              {
                internalType: "uint256",
                name: "value",
                type: "uint256"
              }
            ],
            internalType: "struct AttestationRequestData[]",
            name: "data",
            type: "tuple[]"
          }
        ],
        internalType: "struct MultiAttestationRequest[]",
        name: "multiAttestationRequest",
        type: "tuple[]"
      }
    ],
    name: "submitAttestations",
    outputs: [
      {
        internalType: "bytes32[]",
        name: "",
        type: "bytes32[]"
      }
    ],
    stateMutability: "payable",
    type: "function"
  }
];

// Simple schema encoder for score data
const encodeScoreData = (
  passing_score: boolean,
  score_decimals: bigint,
  scorer_id: bigint,
  score: bigint,
  threshold: bigint,
  stamps: Array<{ provider: string; score: bigint }>
): string => {
  const abiCoder = ethers.AbiCoder.defaultAbiCoder();
  return abiCoder.encode(
    ["bool", "uint8", "uint128", "uint32", "uint32", "tuple(string,uint256)[]"],
    [passing_score, score_decimals, scorer_id, score, threshold, stamps]
  );
};

const getAddresses = async (filePath: string): Promise<string[]> => {
  const addresses: string[] = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity,
  });
  
  for await (const line of rl) {
    const addr = line.trim();
    if (addr && ethers.isAddress(addr)) {
      addresses.push(addr);
    }
  }
  return addresses;
};

const requestV2Score = async (address: string, scorerId: string): Promise<V2ScoreResponseData> => {
  const getScoreUrl = `${SCORER_ENDPOINT}/internal/score/v2/${scorerId}/${address}`;

  try {
    const response = await axios.get(getScoreUrl, {
      headers: {
        Authorization: SCORER_API_KEY,
      },
    });
    return response.data;
  } catch (error) {
    console.error(`Error fetching score for ${address}:`, error);
    throw error;
  }
};

const createAttestationData = (
  address: string,
  scoreData: V2ScoreResponseData,
  scorerId: string
) => {
  const expirationTime = BigInt(Math.floor(new Date(scoreData.expiration_timestamp).getTime() / 1000));
  
  // Parse stamps
  const stamps = Object.entries(scoreData.stamps)
    .filter(([_, { dedup, score }]) => parseFloat(score) > 0 && !dedup)
    .map(([provider, { score }]) => ({
      provider,
      score: parseDecimal(score),
    }));

  const encodedData = encodeScoreData(
    scoreData.passing_score,
    BigInt(SCORE_DECIMALS),
    BigInt(scorerId),
    parseDecimal(scoreData.score),
    parseDecimal(scoreData.threshold),
    stamps
  );

  return {
    recipient: address,
    expirationTime,
    revocable: true,
    refUID: ZERO_BYTES32,
    data: encodedData,
    value: BigInt(0),
  };
};

const writeScoreOnchain = async (
  contract: ethers.Contract,
  address: string,
  scoreData: V2ScoreResponseData,
  scorerId: string
): Promise<string> => {
  try {
    const attestationData = createAttestationData(address, scoreData, scorerId);
    
    const multiAttestationRequest = [
      {
        schema: SCHEMA_UID,
        data: [attestationData],
      },
    ];
    
    const tx = await contract.submitAttestations(multiAttestationRequest);
    await tx.wait();
    
    console.log(`✓ Attestation created for ${address}: ${scoreData.score} (tx: ${tx.hash})`);
    return tx.hash;
  } catch (error) {
    console.error(`✗ Failed to create attestation for ${address}:`, error);
    throw error;
  }
};

const processAddresses = async (
  addresses: string[],
  contract: ethers.Contract,
  scorerId: string
): Promise<Array<{ address: string; score: string; txHash?: string; error?: string }>> => {
  const results: Array<{ address: string; score: string; txHash?: string; error?: string }> = [];
  const batchSize = 5; // Process in smaller batches to avoid rate limits
  
  for (let i = 0; i < addresses.length; i += batchSize) {
    const batch = addresses.slice(i, i + batchSize);
    
    for (const address of batch) {
      try {
        console.log(`Processing ${address}...`);
        
        // Get score from API
        const scoreData = await requestV2Score(address, scorerId);
        
        if (scoreData.error) {
          console.log(`⚠ API error for ${address}: ${scoreData.error}`);
          results.push({ address, score: "0", error: scoreData.error });
          continue;
        }
        
        // Create attestation onchain
        const txHash = await writeScoreOnchain(
          contract,
          address,
          scoreData,
          scorerId
        );
        
        results.push({
          address,
          score: scoreData.score,
          txHash,
        });
        
        // Small delay to avoid overwhelming the API
        await new Promise(resolve => setTimeout(resolve, 1000));
        
      } catch (error) {
        console.error(`Error processing ${address}:`, error);
        results.push({
          address,
          score: "0",
          error: error instanceof Error ? error.message : "Unknown error",
        });
      }
    }
    
    console.log(`Processed batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(addresses.length / batchSize)}`);
  }
  
  return results;
};

const writeCsv = async (results: Array<{ address: string; score: string; txHash?: string; error?: string }>) => {
  const csvContent = [
    "address,score,txHash,error",
    ...results.map(r => `${r.address},${r.score},${r.txHash || ""},${r.error || ""}`)
  ].join("\n");
  
  fs.writeFileSync(OUTPUT_FILE, csvContent);
  console.log(`Results written to ${OUTPUT_FILE}`);
};

const main = async () => {
  try {
    console.log("Starting score attestation processing...");
    
    // Setup provider and contract
    const provider = new ethers.JsonRpcProvider(RPC_URL);
    const wallet = new ethers.Wallet(PRIVATE_KEY, provider);
    const contract = new ethers.Contract(
      ATTESTER_CONTRACT_ADDRESS,
      ATTESTER_ABI,
      wallet
    );
    
    // Read addresses from CSV
    const addresses = await getAddresses(INPUT_FILE);
    console.log(`Found ${addresses.length} addresses to process`);
    
    if (addresses.length === 0) {
      console.log("No valid addresses found in CSV file");
      return;
    }
    
    // Process addresses
    const results = await processAddresses(addresses, contract, COMMUNITY_ID);
    
    // Write results
    await writeCsv(results);
    
    const successful = results.filter(r => r.txHash).length;
    const failed = results.filter(r => r.error).length;
    
    console.log(`\nProcessing complete:`);
    console.log(`✓ Successful: ${successful}`);
    console.log(`✗ Failed: ${failed}`);
    console.log(`📊 Total: ${results.length}`);
    
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  }
};

main().catch(console.error); 