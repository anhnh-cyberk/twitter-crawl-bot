import axios from "axios";
import * as fs from "fs";
import { getMongoConnection } from "../common/connection/mongo-connection";
const client = await getMongoConnection();
export async function makeGetRequest(url: string): Promise<any> {
  const db = client.db("CoinseekerETL");
  const botAccountCollection = db.collection("BotAccount");
  let accountListDicts;
  if (fs.existsSync("account_list.json")) {
    //to use local file, test purpose
    accountListDicts = JSON.parse(
      fs.readFileSync("account_list.json", "utf-8")
    );
  } else {
    const accountList = await botAccountCollection
      .find({ status: { $ne: "error" } })
      .toArray();

    accountListDicts = accountList.map((account) => ({
      username: account.username,
      cookie: account.cookie,
      authorization: account.authorization, 
      "x-csrf-token": account["x-csrf-token"],
      "x-guest-token": account["x-guest-token"],
      "user-agent": account["user-agent"],
    }));
  }

  const randomAccount =
    accountListDicts[Math.floor(Math.random() * accountListDicts.length)];
  console.log(randomAccount.username);

  const headers = {
    Cookie: randomAccount.cookie,
    Authorization: randomAccount.authorization,
    "x-csrf-token": randomAccount["x-csrf-token"],
    "User-Agent": randomAccount["user-agent"],
  };

  try {
    const res = await axios.get(url, { headers });
    return res.data; // Return the response data
  } catch (error) {
    console.error("Error making GET request:", error);
    throw error; // Re-throw the error to be handled by the caller
  }
}
