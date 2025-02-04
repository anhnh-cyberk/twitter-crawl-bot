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

  async upsertMany(records: TwitterAccount[]): Promise<void> {
    return withConnectionRetry(async () => {
      const twitterAccountCollection =
        db.collection<TwitterAccount>("TwitterAccount");
      const operations = records.map((record) => ({
        updateOne: {
          filter: { user_id: record.user_id },
          update: {
            $set: {
              screen_name: record.screen_name,
              avatar_url: record.avatar_url,
              name: record.name,
            },
            $setOnInsert: { status: "new" },
          },
          upsert: true,
        },
      }));
      await twitterAccountCollection.bulkWrite(operations);
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
  async insertMany({ userId, followings }): Promise<boolean> {
    return withConnectionRetry(async () => {
      let existed = false;
      const relationCollection = db.collection<Relation>(
        "TwitterAccountFollowing"
      );
      const operations = followings.map((followingId) => ({
        insertOne: {
          document: {
            user_id: userId,
            following_id: followingId,
          },
          options: { ordered: false },
        },
      }));
      await relationCollection.bulkWrite(operations, { ordered: false });
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

export const UserDAL = {
  // other code
  async getOldRecord(): Promise<any> {
    return withConnectionRetry(async () => {
      const userCollection = db.collection("User");
      const record = await userCollection.findOne({
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
  async getNewRecord(): Promise<any> {
    return withConnectionRetry(async () => {
      const kolCollection = db.collection("TwitterKOL");
      const record = await kolCollection.findOne({
        scanned: { $exists: false },
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
      .find()
      .toArray();
    return records;
  },
  async batchUpdateScannedAt(kolIds: string[]) {
    const now = new Date();
    const kolCollection = db.collection("TwitterKOL");
    await kolCollection.updateMany(
      { user_id: { $in: kolIds } },
      { $set: { scanned_at: now } }
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
