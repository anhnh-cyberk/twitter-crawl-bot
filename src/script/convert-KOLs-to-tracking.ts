interface OriginalKOL {
  _id: {
    $oid: string;
  };
  user_id: string;
  avatar_url: string;
  name: string;
  screen_name: string;
  status: string;
  error?: string;
  transported: boolean;
  scannedAt: string;
  scanned: boolean;
}

interface TransformedKOL {
  twitter_id: string;
  scannedAt: string;
  status: string;
}

function transformKOLData(originalData: OriginalKOL[]): TransformedKOL[] {
  return originalData.map((kol) => ({
    twitter_id: kol.user_id,
    scannedAt: kol.scannedAt,
    status: "completed", // Setting default status as 'completed'
  }));
}

// Example usage
import * as fs from "fs";

// Read the JSON file
const rawData = fs.readFileSync("CoinseekerETL.TwitterKOL.json", "utf-8");
const originalKOLs: OriginalKOL[] = JSON.parse(rawData);

// Transform the data
const transformedKOLs = transformKOLData(originalKOLs);

// Optionally, write to a new file
fs.writeFileSync(
  "transformed_kol.json",
  JSON.stringify(transformedKOLs, null, 2)
);
