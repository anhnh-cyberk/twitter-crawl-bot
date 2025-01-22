import { Db, Collection, ObjectId } from "mongodb";
import { PoolClient } from "pg";
import { setTimeout as sleep } from "timers/promises";
import { DateTime } from "luxon";
import {
  getMongoConnection,
  isMongoAvailable,
} from "../common/connection/mongo-connection";
import { getPostgreConnection } from "../common/connection/postgre-connection";
let pool = getPostgreConnection();
interface FollowingDocument {
  _id: ObjectId;
  user_id: string | number;
  following_id: string | number;
  transported?: boolean;
  error?: string;
}

let client = await getMongoConnection();

async function insertQuery(
  userId: string | number,
  followingId: string | number
): Promise<string> {
  const now: DateTime = DateTime.now();
  const formattedDatetime: string = now.toFormat("yyyy-MM-dd HH:mm:ss.SSS");
  let error: string = "";
  let client: PoolClient | null = null;

  try {
    client = await pool.connect();
    const query: string =
      'INSERT INTO public."TwitterAccountFollowing" ("createdAt", "updatedAt", id, following_id) VALUES($1, $2, $3, $4)';
    await client.query(query, [
      formattedDatetime,
      formattedDatetime,
      userId,
      followingId,
    ]);
    await client.query("COMMIT");
  } catch (e) {
    console.error(`Error inserting data: ${e}`);
    error = `Error: ${e}`;
    if (client) {
      await client.query("ROLLBACK");
    }
  } finally {
    if (client) {
      client.release();
    }
  }
  return error;
}

async function loadFunction(): Promise<void> {
  const db: Db = client.db("CoinseekerETL");
  const relationCollection: Collection<FollowingDocument> = db.collection(
    "TwitterAccountFollowing"
  );

  while (true) {
    const existingDocument: FollowingDocument | null =
      await relationCollection.findOne({ transported: { $exists: false } });

    if (!existingDocument) {
      console.log("No more data to add.");
      break;
    }

    const error: string = await insertQuery(
      existingDocument.user_id,
      existingDocument.following_id
    );

    if (error) {
      console.log(
        existingDocument.user_id,
        existingDocument.following_id,
        " insert failed"
      );
      await relationCollection.updateOne(
        { _id: existingDocument._id },
        {
          $set: {
            transported: false,
            error: error,
          },
        }
      );
    } else {
      console.log(
        existingDocument.user_id,
        existingDocument.following_id,
        " insert successfully"
      );
      await relationCollection.updateOne(
        { _id: existingDocument._id },
        {
          $set: {
            transported: true,
          },
        }
      );
    }
  }
}

async function main(): Promise<void> {
  const delay: number = 1;
  while (true) {
    try {
      await loadFunction();
      console.log(`process completed, retry in next ${delay}s`);
      await sleep(delay * 1000);
    } catch (e: any) {
      console.error("An error occurred:", e);
      // Attempt to reconnect to client and pool if not valid
      if (!isMongoAvailable(client)) {
        try {
          client = await getMongoConnection();
          console.log("Mongo reconnection successful.");
        } catch (mongoReconnectError) {
          console.error("Failed to reconnect to MongoDB:", mongoReconnectError);
        }
      }

      if (!pool) {
        try {
          pool = getPostgreConnection();
          console.log("PostgreSQL reconnection successful.");
        } catch (postgreReconnectError) {
          console.error(
            "Failed to reconnect to PostgreSQL:",
            postgreReconnectError
          );
        }
      }
    }
  }
}

main();
