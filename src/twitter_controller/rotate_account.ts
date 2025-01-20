import axios from "axios";
import { MongoClient } from "mongodb";
import * as fs from "fs";

declare global {
  var client: MongoClient;
}

export async function makeGetRequest(url: string): Promise<any> {
  const db = client.db("CoinseekerETL");
  const botAccountCollection = db.collection("BotAccount");
  const accountList = await botAccountCollection
    .find({ status: { $ne: "error" } })
    .toArray();

  const accountListDicts = accountList.map((account) => ({
    username: account.username,
    cookie: account.cookie,
    authorization: account.authorization,
    "x-csrf-token": account["x-csrf-token"],
    "x-guest-token": account["x-guest-token"],
    "user-agent": account["user-agent"],
  }));

  // Write the account list to a JSON file (if it doesn't exist)
  if (!fs.existsSync("account_list.json")) {
    fs.writeFileSync(
      "account_list.json",
      JSON.stringify(accountListDicts, null, 2)
    );
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
