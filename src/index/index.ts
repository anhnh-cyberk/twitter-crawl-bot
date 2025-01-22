import { Collection, Document } from "mongodb";

import {
  getMongoConnection,
  isMongoAvailable,
} from "../common/connection/mongo-connection";
import { getPostgreConnection } from "../common/connection/postgre-connection";
const pool = getPostgreConnection();
interface User {
  twitter_id: string;
  screen_name: string;
}
let mongoClient = await getMongoConnection();
let db = mongoClient.db("CoinseekerETL");
let userCollection = db.collection("User");

// Initialize MongoDB client

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
  const delay = 1;
  while (true) {
    try {
      const userList = await loadUserFromCoinseeker();
      await insertNewUserToLake(userList);
      console.log(`Process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    } catch (error) {
      console.error("An error occurred:", error);
      validateConnection();
    }
  }
}

async function validateConnection() {
  if (!mongoClient || !isMongoAvailable(mongoClient)) {
    mongoClient = await getMongoConnection();
    db = mongoClient.db(process.env.MONGO_DB_NAME);
  }
}
main();
