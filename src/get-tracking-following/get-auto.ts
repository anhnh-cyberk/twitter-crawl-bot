import { Db, Collection, ObjectId } from "mongodb";
import { setTimeout as sleep } from "timers/promises";
import * as fs from "fs";
import { DateTime } from "luxon";
import { makeGetRequest } from "../twitter-controller/rotate-account.js";
import { getMongoConnection } from "../common/connection/mongo-connection.js";
import { getPostgreConnection } from "../common/connection/postgre-connection.js";
import {
  TwitterAccount,
  TwitterAccountFollowing,
  Tracking,
  User,
  RawDataTwitterUserFriends,
} from "../common/models/mongo-models.js";
import { APIResponse } from "../common/models/api-models.js";
const pool = getPostgreConnection();
const client = getMongoConnection();
interface AutoTracking {
  _id: ObjectId;
  twitter_id: string;
  screen_name: string;
  status: "new" | "completed";
}

async function botFunc(): Promise<void> {
  const db: Db = client.db("CoinseekerETL");
  const autoTrackingCollection: Collection<AutoTracking> =
    db.collection("AutoTracking");

  const newTrackingUser: AutoTracking | null =
    await autoTrackingCollection.findOne({ status: "new" });

  if (!newTrackingUser) {
    console.log("No new user found");
    return;
  }

  console.log(`Processing user ${newTrackingUser.screen_name}`);
  const twitterId: string = newTrackingUser.twitter_id;
  const trackingCollection: Collection<Tracking> = db.collection("Tracking");
  const userCollection: Collection<User> = db.collection("User");
  if (
    (await userCollection.findOne({ twitter_id: twitterId })) ||
    (await trackingCollection.findOne({ twitter_id: twitterId }))
  ) {
    console.log(
      `User ${newTrackingUser.screen_name} already in user/tracking collection`
    );
    await autoTrackingCollection.updateOne(
      { _id: newTrackingUser._id },
      { $set: { status: "completed" } }
    );
    return;
  }

  const data: any[] = await getAndUpdateAllFriends(twitterId);

  const rawCollection: Collection<RawDataTwitterUserFriends> = db.collection(
    "RawData_TwitterUser_Friends"
  );
  await rawCollection.updateOne(
    { user_id: twitterId },
    {
      $set: {
        screen_name: newTrackingUser.screen_name,
        data: data,
      },
    },
    { upsert: true }
  );

  await autoTrackingCollection.updateOne(
    { _id: newTrackingUser._id },
    { $set: { status: "completed" } }
  );
}

async function getAndUpdateAllFriends(userId: string): Promise<any[]> {
  const allRecords: any[] = [];
  let cursor: string | null = "";
  let loop: number = 0;
  const MAX_RECORD_COUNT: number = 100;
  const logFile = fs.createWriteStream("error_get_following.txt", {
    flags: "a",
  });

  while (true) {
    console.log(`${loop}:${cursor}`);
    loop++;

    const randomWaitTime: number = Math.random() * 2 + 2;
    const response: APIResponse | null = await makeApiRequest(userId, cursor);

    await sleep(randomWaitTime * 1000);

    if (response) {
      const arr: any[] = response.data;
      for (const record of arr) {
        try {
          const result = record.content?.itemContent?.user_results?.result;
          const friendId: string = result.rest_id;
          const screenName: string = result.legacy.screen_name;
          const name: string = result.legacy.name;
          const avatarUrl: string = result.legacy.profile_image_url_https;

          await AddTwitterAccount(friendId, screenName, name, avatarUrl);
          await AddRelation(userId, friendId);
        } catch (e: any) {
          if (record.content?.itemContent?.user_results?.result) {
            const timestamp = DateTime.now().toFormat("yyyy-MM-dd HH:mm:ss");
            console.error(
              `Error when parsing data of userId: ${userId}, result: ${record.content?.itemContent?.user_results?.result}, Exception: ${e}`
            );
            logFile.write(
              `Timestamp: ${timestamp}\nError when parsing data of userId: ${userId}, Record: ${JSON.stringify(
                record
              )}, Exception: ${e}\n`
            );
          }
        }
      }
      allRecords.push(...arr);
      cursor = response.next_cursor;
    } else {
      console.error("Error: 'response' is None");
    }

    if (
      !cursor ||
      cursor.startsWith("0|") ||
      allRecords.length > MAX_RECORD_COUNT
    ) {
      break;
    }
  }
  logFile.close();
  return allRecords;
}

async function makeApiRequest(
  userId: string,
  cursor: string | null
): Promise<APIResponse | null> {
  const features: string =
    "&features=%7B%22profile_label_improvements_pcf_label_in_post_enabled%22%3Atrue%2C%22rweb_tipjar_consumption_enabled%22%3Atrue%2C%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Afalse%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22premium_content_api_read_enabled%22%3Afalse%2C%22communities_web_enable_tweet_community_results_fetch%22%3Atrue%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22responsive_web_grok_analyze_button_fetch_trends_enabled%22%3Afalse%2C%22responsive_web_grok_analyze_post_followups_enabled%22%3Atrue%2C%22responsive_web_grok_share_attachment_enabled%22%3Atrue%2C%22articles_preview_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Atrue%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22creator_subscriptions_quote_tweet_preview_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22rweb_video_timestamps_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D";
  let res: any = "";
  try {
    const apiEndpoint: string = `https://x.com/i/api/graphql/pbhw14as2BgZvJAwAbVJpg/Following?variables=%7B%22userId%22%3A%22${userId}%22%2C%22count%22%3A40%2C%22includePromotedContent%22%3Afalse%2C%22cursor%22%3A%22${cursor}%22%7D${features}`;
    res = await makeGetRequest(apiEndpoint);
    const data = await res.json();

    fs.appendFileSync(
      "log.txt",
      `Date: ${DateTime.now().toFormat(
        "yyyy-MM-dd HH:mm:ss"
      )}\nCursor: ${cursor}\nResponse: ${JSON.stringify(data)}\n\n`
    );

    const instructions: any[] =
      data.data.user.result.timeline.timeline.instructions;
    const last: any = instructions[instructions.length - 1];
    const nextCursor: string | null =
      last.entries[last.entries.length - 2].content.value;

    return {
      data: last.entries.slice(0, -2),
      next_cursor: nextCursor,
    };
  } catch (e: any) {
    if (res.status_code === 401 || res.status_code === 403) {
      console.error(`Authorization error: ${e}`);
      return {
        data: [],
        next_cursor: cursor, // Retry current cursor with a different account
      };
    } else {
      console.error(e);
      console.error(`res: ${JSON.stringify(res)}`);
      return {
        data: [],
        next_cursor: null,
      };
    }
  }
}

async function AddTwitterAccount(
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
}

async function AddRelation(userId: string, friendId: string): Promise<void> {
  const db: Db = client.db("CoinseekerETL");
  const relationCollection: Collection<TwitterAccountFollowing> = db.collection(
    "TwitterAccountFollowing"
  );

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
}

async function main(): Promise<void> {
  const delay: number = 1;
  while (true) {
    try {
      await botFunc();
      console.log(`process completed, retry in next ${delay}s`);
      await sleep(delay * 1000);
    } catch (e: any) {
      console.error(`Error when processing ${e}: ${e.stack}`);
      break;
    }
  }
  client.close();
  pool.end();
}

main();
