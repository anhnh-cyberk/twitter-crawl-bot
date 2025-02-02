import { getPostgreConnection } from "../common/connection/postgre-connection";
import { UserDAL } from "./mongo-dal";
const pool = getPostgreConnection();
interface User {
  twitter_id: string;
  screen_name: string;
}

// Initialize MongoDB client
async function insertNewUserToLake(userList: User[]): Promise<void> {
  for (const user of userList) {
    await UserDAL.addUser(user.twitter_id, user.screen_name);
  }
}
async function updateProcessedAt(userList: User[]): Promise<void> {
  let client;
  for (const user of userList) {
    try {
      client = await pool.connect();
      await client.query(
        `UPDATE public."User" SET "processedAt" = CURRENT_TIMESTAMP WHERE "twitter_id" = '${user.twitter_id}' AND "processedAt" IS NULL`
      );
    } catch (e) {
      console.log("Error when update processed at:" + e);
    } finally {
      client.release();
    }
  }
}

async function loadUserFromCoinseeker(): Promise<User[]> {
  let client;
  try {
    client = await pool.connect();
    const result = await client.query(
      'SELECT twitter_id, screen_name FROM public."User" where "processedAt" is null'
    );
    const userList: User[] = result.rows;
    console.log("User List:", userList);
    return userList;
  } catch (error) {
    console.error("Error getting data:", error);
    return [];
  } finally {
    client.release();
  }
}

async function main() {
  const delay = 2;
  while (true) {
    try {
      console.log("waiting for data from coinseeker");
      const userList = await loadUserFromCoinseeker();
      console.log("receive data from coinseeker");
      await insertNewUserToLake(userList);
      await updateProcessedAt(userList);
      console.log(`Process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    } catch (error) {
      console.error("An error occurred:", error);
    }
  }
}

main();
