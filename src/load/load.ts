import { Db, Collection, ObjectId, Filter, UpdateFilter } from "mongodb";
import { PoolClient } from "pg";
import { setTimeout as sleep } from "timers/promises";
import { DateTime } from "luxon";

import {
  getMongoConnection,
  isMongoAvailable,
} from "../common/connection/mongo-connection";
import * as dotenv from "dotenv";
dotenv.config();
import { getPostgreConnection } from "../common/connection/postgre-connection";
let pool = getPostgreConnection();

interface TwitterAccountDocument {
  _id: ObjectId;
  user_id: string | number;
  screen_name: string;
  name: string;
  avatar_url: string;
  transportedDev?: boolean;
  transportedProd?: boolean;
  error?: string;
}
function getFilter(): Filter<TwitterAccountDocument> {
  if (process.env.ENV == "prod") {
    return {
      transportedProd: { $exists: false },
    };
  } else {
    return {
      transportedDev: { $exists: false },
    };
  }
}
function setTransported(error: string): UpdateFilter<Document> {
  if (process.env.ENV == "prod") {
    return {
      $set: {
        transportedProd: !error,
        ...(error && { error }),
      },
    };
  } else {
    return {
      $set: {
        transportedDev: !error,
        ...(error && { error }),
      },
    };
  }
}
let client = await getMongoConnection();

async function insertOneAccount(
  userId: string | number,
  screenName: string,
  name: string,
  avatar: string
): Promise<string> {
  const now: DateTime = DateTime.now();
  const formattedDatetime: string = now.toFormat("yyyy-MM-dd HH:mm:ss.SSS");
  let error: string = "";
  let postgreClient: PoolClient | null = null;

  try {
    postgreClient = await pool.connect();
    avatar.replace(/_normal\./, ".");
    const query: string =
      'INSERT INTO public."TwitterAccount" (id, screen_name, "name", avatar_url, "createdAt", "updatedAt") VALUES ($1, $2, $3, $4, $5, $6)';
    await postgreClient.query(query, [
      userId,
      screenName,
      name,
      avatar,
      formattedDatetime,
      formattedDatetime,
    ]);
    await postgreClient.query("COMMIT");
  } catch (e) {
    console.error(`Error inserting data: ${e}`);
    error = `Error: ${e}`;
    if (postgreClient) {
      await postgreClient.query("ROLLBACK");
    }
  } finally {
    if (postgreClient) {
      postgreClient.release();
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
      await twitterAccountCollection.findOne(getFilter());

    if (!existingDocument) {
      console.log("No more data to add.");
      break;
    }

    const error: string = await insertOneAccount(
      existingDocument.user_id,
      existingDocument.screen_name,
      existingDocument.name,
      existingDocument.avatar_url
    );

    if (error) {
      console.log(existingDocument.screen_name, " insert failed");
      await twitterAccountCollection.updateOne(
        { _id: existingDocument._id },
        setTransported(error)
      );
    } else {
      console.log(existingDocument.screen_name, " insert successfully");
      await twitterAccountCollection.updateOne(
        { _id: existingDocument._id },
        setTransported(error)
      );
    }
  }
}

async function insertBatchAccounts(
  users: TwitterAccountDocument[]
): Promise<string> {
  const now: DateTime = DateTime.now();
  const formattedDatetime: string = now.toFormat("yyyy-MM-dd HH:mm:ss.SSS");
  let error: string = "";
  let postgreClient: PoolClient | null = null;

  try {
    postgreClient = await pool.connect();
    const values = users
      .map(
        (user) =>
          `('${user.user_id}', '${user.screen_name.replace(
            /'/g,
            "''"
          )}', '${user.name.replace(/'/g, "''")}', '${user.avatar_url.replace(
            /_normal\./,
            "."
          )}', '${formattedDatetime}', '${formattedDatetime}')`
      )
      .join(",");
    const query = `INSERT INTO public."TwitterAccount" (id, screen_name, "name", avatar_url, "createdAt", "updatedAt") VALUES ${values}
    ON CONFLICT (id) DO UPDATE SET
        "name" = EXCLUDED."name",
        avatar_url = EXCLUDED.avatar_url,
        "updatedAt" = EXCLUDED."updatedAt"`;
    await postgreClient.query(query);
    await postgreClient.query("COMMIT");
  } catch (e) {
    console.error(`Error inserting data: ${e}`);
    error = `Error: ${e}`;
    if (postgreClient) {
      await postgreClient.query("ROLLBACK");
    }
  } finally {
    if (postgreClient) {
      postgreClient.release();
    }
  }
  return error;
}

async function batchLoadFunction(): Promise<void> {
  const db: Db = client.db("CoinseekerETL");
  const twitterAccountCollection: Collection<TwitterAccountDocument> =
    db.collection("TwitterAccount");

  while (true) {
    const existingDocuments: TwitterAccountDocument[] =
      await twitterAccountCollection
        .find(getFilter())
        .limit(100) // Add a limit to prevent memory issues
        .toArray();

    console.log("count:", existingDocuments.length);
    if (existingDocuments.length === 0) {
      console.log("No more data to add.");
      break;
    }

    const error: string = await insertBatchAccounts(existingDocuments);
    if (error) {
      console.log(existingDocuments.length, "records failed with error", error);
    } else {
      console.log(existingDocuments.length, "records inserted successfully");
    }

    const operations = existingDocuments.map((document) => ({
      updateOne: {
        filter: { _id: document._id },
        update: setTransported(error),
      },
    }));

    await twitterAccountCollection.bulkWrite(operations);
  }
}
async function main(): Promise<void> {
  const delay: number = 3;
  while (true) {
    try {
      await batchLoadFunction();
      console.log(`process completed, retry in next ${delay}s`);
      await sleep(delay * 1000);
    } catch (error) {
      console.error("An error occurred:", error);
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
