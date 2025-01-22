import { Collection, Db, ObjectId } from "mongodb";
import { getMongoConnection } from "../common/connection/mongo-connection";
import {
  AutoTracking,
  TwitterAccount,
  TwitterAccountFollowing
} from "../common/models/mongo-models.js";

const client = await getMongoConnection();
const db = client.db(process.env.MONGO_DB_NAME);

export const AutoTrackingDAL = {
  async upsert(autoTracking: AutoTracking): Promise<void> {
    const userCollection = db.collection<AutoTracking>("AutoTracking");
    await userCollection.updateOne(
      { twitter_id: autoTracking.twitter_id },
      {
        $set: {
          screen_name: autoTracking.screen_name,
          name: autoTracking.name,
        },
        $setOnInsert: { status: "new" },
      },
      { upsert: true }
    );
  },
  async getNewRecord(): Promise<any> {
    const autoTrackingCollection = db.collection("AutoTracking");
    const record = await autoTrackingCollection.findOne({ status: "new" });
    return record;
  },
  async updateStatus(tweeterId: string, status: string): Promise<void> {
    const autoTrackingCollection = db.collection("AutoTracking");
    await autoTrackingCollection.updateOne(
      { twitter_id: tweeterId },
      { $set: { status: status } }
    );
  },
};

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
  async isExists(twitterId: string): Promise<boolean> {
    const trackingCollection = db.collection("Tracking");
    const record = await trackingCollection.findOne({ twitter_id: twitterId });
    return record ? true : false;
  },
};

export const UserDAL = {
  async isExists(twitterId: string): Promise<boolean> {
    const trackingCollection = db.collection("User");
    const record = await trackingCollection.findOne({ twitter_id: twitterId });
    return record ? true : false;
  },
};

export const KOLDal = {
  async isExists(twitterId: string): Promise<boolean> {
    const trackingCollection = db.collection("TwitterKOL");
    const record = await trackingCollection.findOne({ user_id: twitterId });
    return record ? true : false;
  },
};
export const RawDataTwitterUserFriendsDAL = {
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
