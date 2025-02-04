import { Collection, Db, ObjectId } from "mongodb";
import {
  getMongoConnection,
  isMongoAvailable,
  withConnectionRetry,
} from "../common/connection/mongo-connection";
import {
  TwitterAccount,
  TwitterAccountFollowing,
} from "../common/models/mongo-models.js";

let client = await getMongoConnection();
let db = client.db(process.env.MONGO_DB_NAME);

export const TwitterAccountDAL = {
  async BulkAddTwitterAccounts(accounts: TwitterAccount[]): Promise<void> {
    const db: Db = client.db("CoinseekerETL");
    const twitterAccountCollection: Collection<TwitterAccount> =
      db.collection("TwitterAccount");

    const operations = accounts.map((account) => ({
      updateOne: {
        filter: { user_id: account.user_id },
        update: {
          $set: {
            screen_name: account.screen_name,
            avatar_url: account.avatar_url,
            name: account.name,
          },
          $setOnInsert: { status: "new" },
        },
        upsert: true,
      },
    }));

    if (operations.length > 0) {
      await twitterAccountCollection.bulkWrite(operations, { ordered: false });
    }
  },
};
export const RelationDAL = {
  async BulkAddRelations(
    relations: { userId: string; friendId: string }[]
  ): Promise<void> {
    validateConnection();
    const db: Db = client.db("CoinseekerETL");
    const relationCollection: Collection<TwitterAccountFollowing> =
      db.collection("TwitterAccountFollowing");

    const operations = [];

    // Efficiently check for existing relations before inserting
    const existingRelations = await relationCollection
      .find({
        $or: relations.map((rel) => ({
          user_id: rel.userId,
          following_id: rel.friendId,
        })),
      })
      .toArray();

    const existingRelationSet = new Set(
      existingRelations.map((rel) => `${rel.user_id}-${rel.following_id}`)
    );

    for (const relation of relations) {
      const key = `${relation.userId}-${relation.friendId}`;
      if (!existingRelationSet.has(key)) {
        operations.push({
          insertOne: {
            document: {
              user_id: relation.userId,
              following_id: relation.friendId,
            },
          },
        });
      }
    }

    if (operations.length > 0) {
      await relationCollection.bulkWrite(operations, { ordered: false });
    }
  },
};
export const TrackingDAL = {
  // other code
  async getRecentTracking(): Promise<any> {
    validateConnection();
    const userCollection = db.collection("Tracking");
    const recentlyAddedUser = await userCollection.findOne({
      $or: [{ status: "new" }, { status: { $exists: false } }],
    });
    return recentlyAddedUser;
  },

  async UpdateStatus(mongoId: ObjectId, status: string): Promise<void> {
    validateConnection();
    const userCollection = db.collection("Tracking");
    await userCollection.updateOne(
      { _id: mongoId },
      {
        $set: {
          status: status,
        },
      }
    );
  },
  async isExists(twitterId: string): Promise<boolean> {
    validateConnection();
    const trackingCollection = db.collection("Tracking");
    const record = await trackingCollection.findOne({ twitter_id: twitterId });
    return record ? true : false;
  },
};

export const UserDAL = {
  async isExists(twitterId: string): Promise<boolean> {
    validateConnection();
    const trackingCollection = db.collection("User");
    const record = await trackingCollection.findOne({ twitter_id: twitterId });
    return record ? true : false;
  },
};

export const KOLDal = {
  async isExists(twitterId: string): Promise<boolean> {
    validateConnection();
    const trackingCollection = db.collection("TwitterKOL");
    const record = await trackingCollection.findOne({ user_id: twitterId });
    return record ? true : false;
  },
};
export const RawDataTwitterUserFriendsDAL = {
  async upsert(userId: string, screenName: string, data: any): Promise<void> {
    validateConnection();
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

async function validateConnection() {
  if (!client || !isMongoAvailable(client)) {
    client = await getMongoConnection();
    db = client.db(process.env.MONGO_DB_NAME);
  }
}
