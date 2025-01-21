import {
  getMongoConnection,
  TwitterAccount,
  Relation,
} from "../connection/mongo-connection"; // Assuming you use an ODM or create a DAL
import { ObjectId } from "mongodb";

const client = getMongoConnection();
const db = client.db(process.env.MONGO_DB_NAME);

export const TwitterAccountDAL = {
  async upsert(account: TwitterAccount): Promise<void> {
    const twitterAccountCollection =
      db.collection<TwitterAccount>("TwitterAccount");
    await twitterAccountCollection.updateOne(
      { user_id: account.user_id },
      {
        $set: {
          screen_name: account.screen_name,
          avatar_url: account.avatar_url,
          name: account.name,
        },
        $setOnInsert: { status: "new" },
      },
      { upsert: true }
    );
  },
};

export const RelationDAL = {
  async add(relation: Relation): Promise<boolean> {
    let existed = false;
    const relationCollection = db.collection<Relation>(
      "TwitterAccountFollowing"
    );
    const existingDocument = await relationCollection.findOne({
      user_id: relation.user_id,
      following_id: relation.following_id,
    });
    if (!existingDocument) {
      await relationCollection.insertOne(relation);
    } else {
      existed = true;
    }
    return existed;
  },
};

export const RawDataDAL = {
  async upsert(userId: string, screenName: string, data: any): Promise<void> {
    const rawCollection = db.collection("RawData_TwitterUser_Friends");
    await rawCollection.updateOne(
      { user_id: userId },
      {
        $set: {
          screen_name: screenName,
          data: data,
        },
      },
      { upsert: true }
    );
  },
};

export const UserDAL = {
  // other code
  async getOldRecord(): Promise<any> {
    const userCollection = db.collection("User");
    const record = await userCollection.findOne({
      $or: [
        { scannedAt: null },
        { scannedAt: { $exists: false } },
        { scannedAt: { $lt: getYesterdayDate().setHours(0, 0, 0, 0) } },
      ],
    });
    return record;
  },

  async updateScannedAt(mongoId: ObjectId): Promise<void> {
    const userCollection = db.collection("User");
    await userCollection.updateOne(
      { _id: mongoId },
      {
        $set: {
          scannedAt: new Date().toISOString(),
        },
      }
    );
  },
};

export const KolDAL = {
  // other code
  async getOldRecord(): Promise<any> {
    const kolCollection = db.collection("TwitterKOL");
    const record = await kolCollection.findOne({
      $or: [
        { scannedAt: null },
        { scannedAt: { $exists: false } },
        { scannedAt: { $lt: getYesterdayDate().setHours(0, 0, 0, 0) } },
      ],
    });
    return record;
  },

  async updateScannedAt(mongoId: ObjectId): Promise<void> {
    const userCollection = db.collection("TwitterKOL");
    await userCollection.updateOne(
      { _id: mongoId },
      {
        $set: {
          scannedAt: new Date().toISOString(),
        },
      }
    );
  },
};

function getYesterdayDate(): Date {
  const today = new Date();
  const yesterday = new Date(today);

  // Handle January 1st (and other month start cases)
  if (today.getDate() === 1) {
    if (today.getMonth() === 0) {
      // January (month is 0-indexed)
      yesterday.setFullYear(today.getFullYear() - 1); // Go to previous year
      yesterday.setMonth(11); // December (11)
      yesterday.setDate(31); // 31st
    } else {
      yesterday.setMonth(today.getMonth() - 1); // Go to previous month
      const daysInPreviousMonth = new Date(
        yesterday.getFullYear(),
        yesterday.getMonth() + 1,
        0
      ).getDate(); // Get days in previous month
      yesterday.setDate(daysInPreviousMonth); // Last day of previous month
    }
  } else {
    yesterday.setDate(today.getDate() - 1); // Normal case (not the first of the month)
  }

  return yesterday;
}
