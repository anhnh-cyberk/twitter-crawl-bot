import {
  getMongoConnection,
  TwitterAccount,
  Relation,
  AutoTracking,
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
  async add(relation: Relation): Promise<void> {
    const relationCollection = db.collection<Relation>(
      "TwitterAccountFollowing"
    );
    const existingDocument = await relationCollection.findOne({
      user_id: relation.user_id,
      following_id: relation.following_id,
    });
    if (!existingDocument) {
      await relationCollection.insertOne(relation);
    }
  },
};
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
  async getRecentlyAddedUser(): Promise<any> {
    const userCollection = db.collection("User");
    const recentlyAddedUser = await userCollection.findOne({ status: "new" });
    return recentlyAddedUser;
  },

  async updateUserStatus(mongoId: ObjectId, status: string): Promise<void> {
    const userCollection = db.collection("User");
    await userCollection.updateOne(
      { _id: mongoId },
      {
        $set: {
          status: status,
        },
      }
    );
  },
};
