import * as fs from "fs";
import { makeGetRequest } from "../twitter-controller/rotate-account.js";
import {
  TwitterAccountDAL,
  RelationDAL,
  RawDataDAL,
  TrackingDAL,
  KOLDal,
} from "./mongo-dal.js";
import { TwitterAccount } from "../common/models/mongo-models.js";
import { AuthorizationError, ApiError } from "../common/error.js";

// Function to generate the API URL
function generatApiUrl(userId: string, cursor: string | null): string {
  const features =
    "&features=%7B%22profile_label_improvements_pcf_label_in_post_enabled%22%3Atrue%2C%22rweb_tipjar_consumption_enabled%22%3Atrue%2C%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Afalse%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22premium_content_api_read_enabled%22%3Afalse%2C%22communities_web_enable_tweet_community_results_fetch%22%3Atrue%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22responsive_web_grok_analyze_button_fetch_trends_enabled%22%3Afalse%2C%22responsive_web_grok_analyze_post_followups_enabled%22%3Atrue%2C%22responsive_web_grok_share_attachment_enabled%22%3Atrue%2C%22articles_preview_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Atrue%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22creator_subscriptions_quote_tweet_preview_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22rweb_video_timestamps_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D";
  const baseUrl = "https://x.com/i/api/graphql/pbhw14as2BgZvJAwAbVJpg";
  return `${baseUrl}/Following?variables=${encodeURIComponent(
    JSON.stringify({
      userId: userId,
      count: 40,
      includePromotedContent: false,
      cursor: cursor || "",
    })
  )}${features}`;
}

// Function to make API requests with error handling and retries
async function makeApiRequest(apiUrl: string): Promise<any> {
  try {
    const res = await makeGetRequest(apiUrl);
    return res;
  } catch (error: any) {
    if (
      error.response &&
      (error.response.status === 401 || error.response.status === 403)
    ) {
      throw new AuthorizationError(
        `Authorization error: ${error.message}`,
        error.response
      );
    } else {
      throw new ApiError(
        `API request failed: ${error.message}`,
        error.response
      );
    }
  }
}

async function fetchOnePage(
  userId: string,
  cursor: string | null
): Promise<{ data: any[]; nextCursor: string | null }> {
  const apiUrl = generatApiUrl(userId, cursor);
  const response = await makeApiRequest(apiUrl);

  fs.appendFileSync(
    "log.txt",
    `Date: ${new Date().toString()}\nCursor: ${cursor}\nResponse: ${JSON.stringify(
      response
    )}\n\n`
  );

  const instructions = response.data.user.result.timeline.timeline.instructions;
  const last = instructions[instructions.length - 1];
  const nextCursor = last.entries[last.entries.length - 2].content.value;
  return {
    data: last.entries.slice(0, -2),
    nextCursor,
  };
}
async function processBulkRecord(
  userId: string,
  records: any[]
): Promise<void> {
  try {
    const twitterAccounts: TwitterAccount[] = [];
    const relations: { userId: string; friendId: string }[] = [];

    for (const record of records) {
      try {
        // Inner try-catch for individual record errors
        const friendId = record.content.itemContent.user_results.result.rest_id;
        const screenName =
          record.content.itemContent.user_results.result.legacy.screen_name;
        const name = record.content.itemContent.user_results.result.legacy.name;
        const avatarUrl =
          record.content.itemContent.user_results.result.legacy
            .profile_image_url_https;

        twitterAccounts.push({
          user_id: friendId,
          screen_name: screenName,
          name: name,
          avatar_url: avatarUrl,
        });

        relations.push({ userId, friendId });
      } catch (innerError) {
        console.error(
          `Error processing individual friend record for userId: ${userId}, Record: ${JSON.stringify(
            record
          )}, Error: ${innerError}`
        );
        fs.appendFileSync(
          "error_get_following.txt",
          `Timestamp: ${new Date().toString()}\nError processing individual friend record for userId: ${userId}, Record: ${JSON.stringify(
            record
          )}, Error: ${innerError.message}\n`
        );
        // Continue processing other records even if one fails.
      }
    }

    if (twitterAccounts.length > 0) {
      await TwitterAccountDAL.BulkAddTwitterAccounts(twitterAccounts);
    }

    if (relations.length > 0) {
      await RelationDAL.BulkAddRelations(relations);
    }
  } catch (e: any) {
    console.error(
      `Error processing bulk friend records for userId: ${userId}, Error: ${e}`
    );
    fs.appendFileSync(
      "error_get_following.txt",
      `Timestamp: ${new Date().toString()}\nError processing bulk friend records for userId: ${userId}, Error: ${
        e.message
      }\n`
    );
  }
}
async function fetchAndProcessAllPage(userId: string): Promise<any[]> {
  const allRecords: any[] = [];
  let cursor: string | null = "";
  let loop = 0;
  const MAX_RECORD_COUNT = 1000;

  while (true) {
    console.log(`${loop}:${cursor}`);
    loop += 1;

    try {
      const { data, nextCursor } = await fetchOnePage(userId, cursor);
      processBulkRecord(userId, data);
      allRecords.push(...data);
      cursor = nextCursor;
    } catch (error) {
      if (error instanceof AuthorizationError) {
        console.error(`Authorization error: ${error.message}`);
        cursor = error.response?.data?.next_cursor || cursor; // Retry with the same cursor if provided
        continue; // Retry the request
      } else if (error instanceof ApiError) {
        console.error(`API request error: ${error.message}`);
        console.error(`Response: ${error.response}`);
      } else {
        console.error(`Unexpected error: ${error}`);
      }
      cursor = null; // Stop on error
    }

    if (
      !cursor ||
      cursor.startsWith("0|") ||
      allRecords.length > MAX_RECORD_COUNT
    ) {
      break;
    }
    // Generate a random floating-point number between 2 and 4 (inclusive)
    const randomWaitTime = Math.random() * 2 + 2;
    // Pause execution for the random amount of time
    await new Promise((resolve) => setTimeout(resolve, randomWaitTime * 1000));
  }

  return allRecords;
}
async function botFunction() {
  const recentlyAddedTracking = await TrackingDAL.getRecentTracking();
  if (!recentlyAddedTracking) {
    console.log("No new user found");
    return;
  }
  await TrackingDAL.UpdateStatus(recentlyAddedTracking._id, "processing");
  const data = await fetchAndProcessAllPage(recentlyAddedTracking.twitter_id);

  await RawDataDAL.upsert(
    recentlyAddedTracking.twitter_id,
    recentlyAddedTracking.screen_name,
    data
  );

  await TrackingDAL.UpdateStatus(recentlyAddedTracking._id, "completed");
}

async function main() {
  const delay = 5*60;
  while (true) {
    try {
      await botFunction();
      console.log(`process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    } catch (e) {
      console.log(`Error occure:` + e);
    }
  }
}

main();
