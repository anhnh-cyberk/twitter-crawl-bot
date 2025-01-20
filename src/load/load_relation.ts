import { MongoClient, Db, Collection, ObjectId } from 'mongodb';
import { Pool, PoolClient, QueryResult } from 'pg';
import { setTimeout as sleep } from 'timers/promises';
import { DateTime } from 'luxon';
import * as fs from 'fs';

interface ConnectionData {
  host: string;
  database: string;
  user: string;
  password: string;
  port: number;
}

interface FollowingDocument {
  _id: ObjectId;
  user_id: string | number;
  following_id: string | number;
  transported?: boolean;
  error?: string;
}

const connectionData: ConnectionData = JSON.parse(fs.readFileSync('connection.json', 'utf-8'));

const client: MongoClient = new MongoClient('mongodb://localhost:27017/');
const pool: Pool = new Pool({
  host: connectionData.host,
  database: connectionData.database,
  user: connectionData.user,
  password: connectionData.password,
  port: connectionData.port,
});

async function insertQuery(userId: string | number, followingId: string | number): Promise<string> {
  const now: DateTime = DateTime.now();
  const formattedDatetime: string = now.toFormat("yyyy-MM-dd HH:mm:ss.SSS");
  let error: string = "";
  let client: PoolClient | null = null; 

  try {
    client = await pool.connect();
    const query: string = 'INSERT INTO public."TwitterAccountFollowing" ("createdAt", "updatedAt", id, following_id) VALUES($1, $2, $3, $4)';
    await client.query(query, [formattedDatetime, formattedDatetime, userId, followingId]);
    await client.query('COMMIT');
  } catch (e) {
    console.error(`Error inserting data: ${e}`);
    error = `Error: ${e}`;
    if (client) {
      await client.query('ROLLBACK');
    }
  } finally {
    if (client) {
      client.release();
    }
  }
  return error;
}

async function loadFunction(): Promise<void> {
  const db: Db = client.db('CoinseekerETL');
  const relationCollection: Collection<FollowingDocument> = db.collection('TwitterAccountFollowing');

  while (true) {
    const existingDocument: FollowingDocument | null = await relationCollection.findOne({ transported: { $exists: false } });

    if (!existingDocument) {
      console.log("No more data to add.");
      break;
    }

    const error: string = await insertQuery(existingDocument.user_id, existingDocument.following_id);

    if (error) {
      console.log(existingDocument.user_id, existingDocument.following_id, " insert failed");
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
      console.log(existingDocument.user_id, existingDocument.following_id, " insert successfully");
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
    } catch (error) {
      console.error("An error occurred:", error);
      break;
    }
  }
  client.close();
  pool.end();
}

main();