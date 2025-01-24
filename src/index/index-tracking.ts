import { getPostgreConnection } from "../common/connection/postgre-connection";
import { MongoClient } from "mongodb";
const pool = getPostgreConnection();

async function insertNewUserToLake(trackingList: string[]): Promise<void> {
  const mongo_host = process.env.MONGO_HOST;
  const mongo_port = process.env.MONGO_PORT || 27017;
  const mongo_user = process.env.MONGO_USER; // Your application's MongoDB username
  const mongo_password = process.env.MONGO_PASSWORD; // Your application's MongoDB password
  const mongo_auth_db = process.env.MONGO_AUTH_DB; // Authentication database
  const mongo_uri = `mongodb://${mongo_user}:${mongo_password}@${mongo_host}:${mongo_port}/${mongo_auth_db}`;
  const mongoClient = new MongoClient(mongo_uri);
  mongoClient
    .connect()
    .then(() => console.log("MongoDB client connected successfully"))
    .catch((error) => console.error("MongoDB client connection error:", error));
  let db = mongoClient.db("CoinseekerETL");
  let trackingCollection = db.collection("Tracking");
  try {
    for (const userId of trackingList) {
      await trackingCollection.updateOne(
        { twitter_id: userId },
        { $setOnInsert: { twitter_id: userId } },
        { upsert: true }
      );
    }
  } catch (error) {
    console.error("Error inserting data:", error);
  } finally {
    mongoClient.close();
  }
}

async function loadTrackingFromCoinseeker(): Promise<string[]> {
  let client;
  try {
    client = await pool.connect();
    const result = await client.query(
      `SELECT distinct "twitterAccountId" FROM public."Tracking"`
    );
    const trackingList: string[] = result.rows
      .map((item) => {
        try {
          return item.twitterAccountId;
        } catch (error) {
          console.error("Error mapping item:", error);
          return null;
        }
      })
      .filter(Boolean);
    console.log("Tracking List:", trackingList);
    return trackingList;
  } catch (error) {
    console.error("Error getting data:", error);
    return [];
  } finally {
    if (client) {
      client.release(); // Release the connection back to the pool
    }
  }
}

async function main() {
  const delay = 30;
  while (true) {
    try {
      const trackingList = await loadTrackingFromCoinseeker();
      await insertNewUserToLake(trackingList);
      console.log(`Process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    } catch (error) {
      console.error("An error occurred:", error);
    }
  }
}

main();
