import * as fs from "fs";
import { MongoClient } from "mongodb";
import { makeGetRequest } from "../twitter_controller/rotate_account.js";

async function getAndUpdateAllFriends(userId: string): Promise<any[]> {
  const allRecords: any[] = [];
  let cursor: string | null = ""; // Start without a cursor
  let loop = 0;
  const MAX_RECORD_COUNT = 1000;
  // cursor = "1811011650233850728%7C1878699358648532940";

  // TypeScript doesn't have a direct equivalent to Python's 'with open'.
  // You'll need to use the fs module for file handling.
  const file = fs.createWriteStream("error_get_following.txt", {
    flags: "a",
    encoding: "utf-8",
  });

  try {
    while (true) {
      console.log(`${loop}:${cursor}`);
      loop += 1;

      // Generate a random floating-point number between 2 and 4 (inclusive)
      const randomWaitTime = Math.random() * (4 - 2) + 2;
      const response = await makeApiRequest(userId, cursor);

      // Pause execution for the random amount of time
      await new Promise((resolve) =>
        setTimeout(resolve, randomWaitTime * 1000)
      );

      const autotracking = cursor === ""; // auto tracking for first page

      if (response !== null) {
        const arr = response.data;
        for (const record of arr) {
          try {
            const friendId =
              record.content.itemContent.user_results.result.rest_id;
            const screenName =
              record.content.itemContent.user_results.result.legacy.screen_name;
            const name =
              record.content.itemContent.user_results.result.legacy.name;
            const avatarUrl =
              record.content.itemContent.user_results.result.legacy
                .profile_image_url_https;

            await AddTwitterAccount(friendId, screenName, name, avatarUrl);
            await AddRelation(userId, friendId);

            if (autotracking) {
              await AddAutoTracking(friendId, screenName, name);
            }
          } catch (e: any) {
            console.error(
              `Error when parsing data of userId: ${userId}, Record: ${record}, Exception: ${e}`
            );
            file.write(
              `Timestamp: ${new Date().toString()}\nError when parsing data of userId: ${userId}, Record: ${record}, Exception: ${
                e.message
              }\n`
            );
          }
        }

        allRecords.push(...arr);
        cursor = response.next_cursor || null;
      } else {
        console.error("Error: 'response' is null");
      }

      if (!cursor) {
        // No more cursor means we've reached the end
        break;
      }

      if (cursor.startsWith("0|")) {
        break;
      }

      if (allRecords.length > MAX_RECORD_COUNT) {
        break;
      }
    }
  } finally {
    file.end();
  }

  return allRecords;
}

// Assuming you have a MongoDB client initialized as 'client'

declare global {
  var client: MongoClient;
}

async function AddTwitterAccount(
  userId: string,
  screenName: string,
  name: string,
  avatarUrl: string
): Promise<void> {
  const db = client.db("CoinseekerETL"); // Replace 'CoinseekerETL' with your actual database name
  const twitterAccountCollection = db.collection("TwitterAccount");

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
  const db = client.db("CoinseekerETL");
  const relationCollection = db.collection("TwitterAccountFollowing");

  const existingDocument = await relationCollection.findOne({
    user_id: userId,
    following_id: friendId,
  });

  if (!existingDocument) {
    const document = {
      user_id: userId,
      following_id: friendId,
    };
    await relationCollection.insertOne(document);
  }
}

async function AddAutoTracking(
  userId: string,
  screenName: string,
  name: string
): Promise<void> {
  const db = client.db("CoinseekerETL");
  const userCollection = db.collection("AutoTracking");

  await userCollection.updateOne(
    { twitter_id: userId },
    {
      $set: {
        screen_name: screenName,
        name: name,
      },
      $setOnInsert: { status: "new" },
    },
    { upsert: true }
  );
}


async function makeApiRequest(
  userId: string,
  cursor: string | null
): Promise<any> {
  const features =
    "&features=%7B%22profile_label_improvements_pcf_label_in_post_enabled%22%3Atrue%2C%22rweb_tipjar_consumption_enabled%22%3Atrue%2C%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Afalse%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22premium_content_api_read_enabled%22%3Afalse%2C%22communities_web_enable_tweet_community_results_fetch%22%3Atrue%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22responsive_web_grok_analyze_button_fetch_trends_enabled%22%3Afalse%2C%22responsive_web_grok_analyze_post_followups_enabled%22%3Atrue%2C%22responsive_web_grok_share_attachment_enabled%22%3Atrue%2C%22articles_preview_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Atrue%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22creator_subscriptions_quote_tweet_preview_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22rweb_video_timestamps_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D";

  try {
    const apiEndpoint = `https://x.com/i/api/graphql/pbhw14as2BgZvJAwAbVJpg/Following?variables=%7B%22userId%22%3A%22${userId}%22%2C%22count%22%3A40%2C%22includePromotedContent%22%3Afalse%2C%22cursor%22%3A%22${cursor}%22%7D${features}`;
    const res = await makeGetRequest(apiEndpoint);

    // Log the response data
    fs.appendFileSync(
      "log.txt",
      `Date: ${new Date().toString()}\nCursor: ${cursor}\nResponse: ${JSON.stringify(
        res
      )}\n\n`
    );

    const instructions = res.data.user.result.timeline.timeline.instructions;
    const last = instructions[instructions.length - 1];
    const nextCursor = last.entries[last.entries.length - 2].content.value;

    return {
      data: last.entries.slice(0, -2),
      next_cursor: nextCursor,
    };
  } catch (error: any) {
    if (
      error.response &&
      (error.response.status === 401 || error.response.status === 403)
    ) {
      console.error(`Authorization error: ${error.message}`);
      return { data: [], next_cursor: cursor }; // Retry with the same cursor
    } else {
      console.error(error); // Log the full error object for debugging
      console.error(`Response: ${error.response}`);
      return { data: [], next_cursor: null };
    }
  }
}
