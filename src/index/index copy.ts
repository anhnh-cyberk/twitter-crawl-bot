import { getMongoConnection } from "../common/connection/mongo-connection";
import { getPostgreConnection } from "../common/connection/postgre-connection";
import { MongoClient } from "mongodb";
const pool = getPostgreConnection();
interface User {
  twitter_id: string;
  screen_name: string;
}

// Initialize MongoDB client

async function insertNewUserToLake(userList: User[]): Promise<void> {
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
  let userCollection = db.collection("User");
  try {
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
  } catch (error) {
    console.error("Error inserting data:", error);
  } finally {
    mongoClient.close();
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

async function main() {
  const delay = 1;
  while (true) {
    try {
      const userList = await loadUserFromCoinseeker();
      await insertNewUserToLake(userList);
      console.log(`Process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    } catch (error) {
      console.error("An error occurred:", error);
    }
  }
}

// async function validateConnection() {
//   try {
//     if (!mongoClient || !isMongoAvailable(mongoClient)) {
//       mongoClient = await getMongoConnection();
//       db = mongoClient.db(process.env.MONGO_DB_NAME);
//     }
//   } catch (e) {
//     console.log("Error when validate connection:" + e);
//   }
// }
main();
