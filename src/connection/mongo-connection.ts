import { MongoClient, Db } from "mongodb";
import * as dotenv from "dotenv";
dotenv.config();

export interface TwitterAccount {
  user_id: string;
  screen_name: string;
  name: string;
  avatar_url: string;
  status?: string; // Optional: e.g., "new", "processing", "completed"
}

export interface Relation {
  user_id: string;
  following_id: string;
}

export interface AutoTracking {
  twitter_id: string;
  screen_name: string;
  name: string;
  status?: string; // Optional: e.g., "new", "processing", "completed"
}

export interface User {
  twitter_id: string;
  screen_name: string;
  name: string;
  status?: string; // Optional: e.g., "new", "processing", "completed"
}

export function getMongoConnection() {
  const mongo_host = process.env.MONGO_HOST;
  const mongo_port = process.env.MONGO_PORT || 27017;
  const mongo_user = process.env.MONGO_USER; // Your application's MongoDB username
  const mongo_password = process.env.MONGO_PASSWORD; // Your application's MongoDB password
  const mongo_auth_db = process.env.MONGO_AUTH_DB; // Authentication database

  const mongo_uri = `mongodb://${mongo_user}:${mongo_password}@${mongo_host}:${mongo_port}/${mongo_auth_db}`;
  console.log(mongo_uri);
  const mongoClient = new MongoClient(mongo_uri);
  mongoClient
    .connect()
    .then(() => console.log("MongoDB client connected successfully"))
    .catch((error) => console.error("MongoDB client connection error:", error));
  return mongoClient;
}
