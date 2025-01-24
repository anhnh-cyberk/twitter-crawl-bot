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
  async AddTwitterAccount(
    userId: string,
    screenName: string,
    name: string,
    avatarUrl: string
  ): Promise<void> {
    const db: Db = client.db("CoinseekerETL");
    const twitterAccountCollection: Collection<TwitterAccount> =
      db.collection("TwitterAccount");

    await twitterAccountCollection.updateOne(
      { user_id: userId },
      {
        $set: {
          screen_name: screenName,
          avatar_url: avatarUrl,
          name: name,
        },
        $setOnInsert: { status: "new" },
      },
      { upsert: true }
    );
  },
};
export const RelationDAL = {
  async AddRelation(userId: string, friendId: string): Promise<void> {
    validateConnection();
    const db: Db = client.db("CoinseekerETL");
    const relationCollection: Collection<TwitterAccountFollowing> =
      db.collection("TwitterAccountFollowing");

    const existingDocument: TwitterAccountFollowing | null =
      await relationCollection.findOne({
        user_id: userId,
        following_id: friendId,
      });

    if (!existingDocument) {
      const document: TwitterAccountFollowing = {
        _id: new ObjectId(),
        user_id: userId,
        following_id: friendId,
      };
      await relationCollection.insertOne(document);
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
