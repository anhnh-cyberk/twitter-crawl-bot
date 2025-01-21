import * as fs from "fs";
import { makeGetRequest } from "../twitter-controller/rotate-account.js";
import {
  TwitterAccountDAL,
  RelationDAL,
  RawDataDAL,
  UserDAL,
} from "./mongo-dal.js";
import { AuthorizationError, ApiError } from "../common/error.js";

// Function to generate the API URL
function generateApiUrl(userId: string, cursor: string | null): string {
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
  const apiUrl = generateApiUrl(userId, cursor);
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

//insert friend record, return true if already existed
async function processOneRecord(userId: string, record: any): Promise<boolean> {
  try {
    const friendId = record.content.itemContent.user_results.result.rest_id;
    const screenName =
      record.content.itemContent.user_results.result.legacy.screen_name;
    const name = record.content.itemContent.user_results.result.legacy.name;
    const avatarUrl =
      record.content.itemContent.user_results.result.legacy
        .profile_image_url_https;

    // Use the DAL or ODM to interact with the database
    await TwitterAccountDAL.upsert({
      user_id: friendId,
      screen_name: screenName,
      name,
      avatar_url: avatarUrl,
    });
    let relationExisted = await RelationDAL.add({
      user_id: userId,
      following_id: friendId,
    });
    return relationExisted;
  } catch (e: any) {
    console.error(
      `Error processing friend record for userId: ${userId}, Record: ${JSON.stringify(
        record
      )}, Error: ${e}`
    );
    // Log the error to a file (consider using a proper logging library)
    fs.appendFileSync(
      "error_get_following.txt",
      `Timestamp: ${new Date().toString()}\nError processing friend record for userId: ${userId}, Record: ${JSON.stringify(
        record
      )}, Error: ${e.message}\n`
    );
    return false;
  }
}

async function getAndProcessAllPage(userId: string): Promise<any[]> {
  const allRecords: any[] = [];
  let cursor: string | null = "";
  let loop = 0;
  const MAX_RECORD_COUNT = 1000;
  let needFetchMore = true;
  while (needFetchMore) {
    loop += 1;
    try {
      const delay = 10;
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
      const { data, nextCursor } = await fetchOnePage(userId, cursor);

      for (const record of data) {
        const existed = await processOneRecord(userId, record);
        if (existed) {
          needFetchMore = false;
          console.log(`All follower of ${userId} has been added`);
          break;
        }
      }

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
  const oldRecord = await UserDAL.getOldRecord();
  if (!oldRecord) {
    console.log("No kol need updated");
    return;
  }

  const data = await getAndProcessAllPage(oldRecord.twitter_id);

  await RawDataDAL.upsert(oldRecord.twitter_id, oldRecord.screen_name, data);

  await UserDAL.updateScannedAt(oldRecord._id);
}

async function main() {
  const delay = 10;
  while (true) {
    try {
      await botFunction();
      console.log(`process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    } catch {
      break;
    }
  }
}

main();
