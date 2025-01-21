import { Db, Collection, ObjectId } from "mongodb";
import { PoolClient } from "pg";
import { setTimeout as sleep } from "timers/promises";
import { DateTime } from "luxon";

import { getMongoConnection } from "../connection/mongo-connection";

import { getPostgreConnection } from "../connection/postgre-connection";
const pool = getPostgreConnection();

interface TwitterAccountDocument {
  _id: ObjectId;
  user_id: string | number;
  screen_name: string;
  name: string;
  avatar_url: string;
  transported?: boolean;
  error?: string;
}

const client = getMongoConnection();

async function insertQuery(
  userId: string | number,
  screenName: string,
  name: string,
  avatar: string
): Promise<string> {
  const now: DateTime = DateTime.now();
  const formattedDatetime: string = now.toFormat("yyyy-MM-dd HH:mm:ss.SSS");
  let error: string = "";
  let client: PoolClient | null = null;

  try {
    client = await pool.connect();
    const query: string =
      'INSERT INTO public."TwitterAccount" (id, screen_name, "name", avatar_url, "createdAt", "updatedAt") VALUES ($1, $2, $3, $4, $5, $6)';
    await client.query(query, [
      userId,
      screenName,
      name,
      avatar,
      formattedDatetime,
      formattedDatetime,
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
  const twitterAccountCollection: Collection<TwitterAccountDocument> =
    db.collection("TwitterAccount");

  while (true) {
    const existingDocument: TwitterAccountDocument | null =
      await twitterAccountCollection.findOne({
        transported: { $exists: false },
      });

    if (!existingDocument) {
      console.log("No more data to add.");
      break;
    }

    const error: string = await insertQuery(
      existingDocument.user_id,
      existingDocument.screen_name,
      existingDocument.name,
      existingDocument.avatar_url
    );

    if (error) {
      console.log(existingDocument.screen_name, " insert failed");
      await twitterAccountCollection.updateOne(
        { _id: existingDocument._id },
        {
          $set: {
            transported: false,
            error: error,
          },
        }
      );
    } else {
      console.log(existingDocument.screen_name, " insert successfully");
      await twitterAccountCollection.updateOne(
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
    } catch (error) {
      console.error("An error occurred:", error);
      break;
    }
  }
  client.close();
  pool.end();
}

main();
