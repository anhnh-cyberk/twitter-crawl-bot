import {
  getMongoConnection,
  isMongoAvailable,
  withConnectionRetry,
} from "../common/connection/mongo-connection"; // Assuming you use an ODM or create a DAL

let client = await getMongoConnection();
let db = client.db(process.env.MONGO_DB_NAME);

export const UserDAL = {
  // other code
  async addUser(twitter_id, screen_name): Promise<void> {
    // validateConnection();
    return withConnectionRetry(async () => {
      const userCollection = db.collection("User");
      await userCollection.updateOne(
        { twitter_id: twitter_id },
        {
          $set: {
            screen_name: screen_name,
          },
          $setOnInsert: { status: "new" },
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
