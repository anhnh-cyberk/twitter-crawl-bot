import { MongoClient, Collection, Document } from "mongodb";
import pg from "pg";
import * as dotenv from "dotenv";
dotenv.config();

const pool = new pg.Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT || "5432"),
  ssl: {
    rejectUnauthorized: false,
  },
});
interface User {
  twitter_id: string;
  screen_name: string;
}

let userCollection: Collection<Document>;
// Initialize MongoDB client
const mongoClient = new MongoClient("mongodb://localhost:27017/");

async function insertNewUserToLake(userList: User[]): Promise<void> {
  for (const user of userList) {
    // console.log(user[1]);
    await userCollection.updateOne(
      { twitter_id: user.twitter_id },
      {
        $set: {
          screen_name: user.screen_name,
        },
        $setOnInsert: { status: "new" },
      },
      { upsert: true }
    );
  }
}

async function loadUserFromCoinseeker(): Promise<User[]> {
  try {
    const client = await pool.connect();
    const result = await client.query(
      'SELECT twitter_id, screen_name FROM public."User"'
    );
    const userList: User[] = result.rows;
    console.log("User List:", userList);
    return userList;
  } catch (error) {
    console.error("Error getting data:", error);
    return [];
  }
}

async function main(): Promise<void> {
  await mongoClient.connect();
  const db = mongoClient.db("CoinseekerETL");
  userCollection = db.collection("User");
  const delay = 1;
  while (true) {
    try {
      const userList = await loadUserFromCoinseeker();
      await insertNewUserToLake(userList);
      console.log(`Process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    } catch (error) {
      console.error("An error occurred:", error);
      break;
    }
  }

  await pool.end();
  await mongoClient.close();
}

main().catch(console.error);
