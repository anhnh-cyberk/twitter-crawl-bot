import {
  getMongoConnection,
  withConnectionRetry,
} from "../common/connection/mongo-connection";
import { TwitterAccount, Relation } from "../common/models/mongo-models";
import { ObjectId } from "mongodb";

const client = await getMongoConnection();
const db = client.db(process.env.MONGO_DB_NAME);

export const TwitterAccountDAL = {
  async upsert(account: TwitterAccount): Promise<void> {
    return withConnectionRetry(async () => {
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
    });
  },
};

export const RelationDAL = {
  async add(relation: Relation): Promise<boolean> {
    return withConnectionRetry(async () => {
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
    });
  },
};

export const RawDataDAL = {
  async upsert(userId: string, screenName: string, data: any): Promise<void> {
    return withConnectionRetry(async () => {
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
    });
  },
};

export const TrackingDAL = {
  // other code
  async getOldRecord(): Promise<any> {
    return withConnectionRetry(async () => {
      const userCollection = db.collection("Tracking");
      const record = await userCollection.findOne({
        $and: [
          {
            $or: [
              { scannedAt: null },
              { scannedAt: { $exists: false } },
              { scannedAt: { $lt: getYesterdayDate().setHours(0, 0, 0, 0) } },
            ],
          },
          { status: "completed" },
        ],
      });
      return record;
    });
  },
  async updateScannedAt(mongoId: ObjectId): Promise<void> {
    return withConnectionRetry(async () => {
      const userCollection = db.collection("Tracking");
      await userCollection.updateOne(
        { _id: mongoId },
        {
          $set: {
            scannedAt: new Date().toISOString(),
          },
        }
      );
    });
  },
};

export const UserDAL = {
  // other code
  async getOldRecord(): Promise<any> {
    return withConnectionRetry(async () => {
      const userCollection = db.collection("User");
      const record = await userCollection.findOne({
        $and: [
          {
            $or: [
              { scannedAt: null },
              { scannedAt: { $exists: false } },
              { scannedAt: { $lt: getYesterdayDate().setHours(0, 0, 0, 0) } },
            ],
          },
          { status: "completed" },
        ],
      });
      return record;
    });
  },
  async updateScannedAt(mongoId: ObjectId): Promise<void> {
    return withConnectionRetry(async () => {
      const userCollection = db.collection("User");
      await userCollection.updateOne(
        { _id: mongoId },
        {
          $set: {
            scanned: true,
            scannedAt: new Date().toISOString(),
          },
        }
      );
    });
  },
};

export const KolDAL = {
  // other code
  async getOldRecord(): Promise<any> {
    return withConnectionRetry(async () => {
      const kolCollection = db.collection("TwitterKOL");
      const record = await kolCollection
        // .findOne({
        //   scanned: { $exists: false },
        // });
        .findOne({
          $or: [
            { scannedAt: null },
            { scannedAt: { $exists: false } },
            { scannedAt: { $lt: getYesterdayDate().setHours(0, 0, 0, 0) } },
          ],
        });
      return record;
    });
  },

  async updateScannedAt(mongoId: ObjectId): Promise<void> {
    return withConnectionRetry(async () => {
      const userCollection = db.collection("TwitterKOL");
      await userCollection.updateOne(
        { _id: mongoId },
        {
          $set: {
            scanned: true,
            scannedAt: new Date().toISOString(),
          },
        }
      );
    });
  },

  async getAllOldRecords(): Promise<any[]> {
    const kolCollection = db.collection("TwitterKOL");
    const records = await kolCollection
      .find({
        $or: [
          { scanned_at: { $lt: getYesterdayDate().setHours(0, 0, 0, 0) } },
          { scanned_at: { $exists: false } },
        ],
      })
      .toArray();
    return records;
  },
  async batchUpdateScannedAt(kolIds: string[]) {
    const now = new Date();
    const kolCollection = db.collection("TwitterKOL");
    await kolCollection.updateMany(
      { user_id: { $in: kolIds } },
      { $set: { scanned_at: now, error: undefined } }
    );
  },
  async batchUpdateErrorCode(kolIds: string[]) {
    const now = new Date();
    const kolCollection = db.collection("TwitterKOL");
    await kolCollection.updateMany(
      { user_id: { $in: kolIds } },
      { $set: { scanned_at: now, error: "DailyUpdateError" } }
    );
  },

  // async batchUpdateErrorCode(): Promise<any[]> {},
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
