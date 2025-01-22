import { MongoClient, Db } from "mongodb";
import * as dotenv from "dotenv";
dotenv.config();

let client: MongoClient | null = null;
let connectionStatus = "disconnected";
let connectionError = null;
let retryCount = 0;
const maxRetries = 3; // Or configure from environment
const retryDelay = 2000; // Milliseconds

export async function getMongoConnection() {
  if (client && connectionStatus === "connected") {
    return client;
  }

  if (connectionStatus === "connecting" || retryCount >= maxRetries) {
    throw new Error(
      connectionError ||
        "MongoDB connection is still being established or max retries reached"
    );
  }

  connectionStatus = "connecting";
  connectionError = null;
  retryCount++;

  const mongo_host = process.env.MONGO_HOST;
  const mongo_port = process.env.MONGO_PORT || 27017;
  const mongo_user = process.env.MONGO_USER; // Your application's MongoDB username
  const mongo_password = process.env.MONGO_PASSWORD; // Your application's MongoDB password
  const mongo_auth_db = process.env.MONGO_AUTH_DB; // Authentication database
  const mongo_uri = `mongodb://${mongo_user}:${mongo_password}@${mongo_host}:${mongo_port}/${mongo_auth_db}`;
  console.log(mongo_uri);
  try {
    const newClient = new MongoClient(mongo_uri);
    newClient
      .connect()
      .then(() => console.log("MongoDB client connected successfully"))
      .catch((error) =>
        console.error("MongoDB client connection error:", error)
      );
    newClient.on("close", () => {
      console.warn("MongoDB connection closed unexpectedly.");
      connectionStatus = "disconnected";
      client = null;
      connectionError = "Connection closed";
    });

    newClient.on("error", (err) => {
      console.error("MongoDB connection error:", err);
      connectionStatus = "disconnected";
      client = null;
      connectionError = err.message || "Connection error";
    });
    client = newClient;
    connectionStatus = "connected";
    retryCount = 0; // Reset on successful connection
    console.log("MongoDB connected successfully.");
    return client;
  } catch (err) {
    console.error(`MongoDB connection attempt ${retryCount} failed:`, err);
    connectionStatus = "disconnected";
    connectionError = err.message || "Failed to connect to MongoDB";

    if (retryCount < maxRetries) {
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
      return getMongoConnection(); // Recursive retry
    } else {
      console.error("Max retries reached. Giving up on MongoDB connection.");
      throw new Error(connectionError);
    }
  }
}
export async function isMongoAvailable(client: MongoClient): Promise<boolean> {
  try {
    await client.db("admin").command({ ping: 1 });
    return true;
  } catch (err) {
    console.error("Error connecting to MongoDB:", err);
    return false;
  }
}

export async function withConnectionRetry(fn) {
  const maxRetries = 3;
  const retryDelay = 1000;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const client = await getMongoConnection();
      const db = client.db(process.env.MONGO_DB_NAME);
      return await fn(db); // Pass the db object to the DAL function
    } catch (error) {
      console.error(`Attempt ${attempt} failed:`, error);

      if (attempt === maxRetries) {
        throw new Error("Max retries reached: " + error.message);
      }

      console.log(`Retrying in ${retryDelay}ms...`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
    }
  }
}
