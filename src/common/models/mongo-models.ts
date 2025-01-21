import { ObjectId } from "mongodb";

export interface TwitterAccount {
  user_id: string;
  screen_name: string;
  name: string;
  avatar_url: string;
  status?: string; // Optional: e.g., "new", "processing", "completed"
}

export interface Relation {
  user_id: string;
  following_id: string;
}

export interface AutoTracking {
  twitter_id: string;
  screen_name: string;
  name: string;
  status?: string; // Optional: e.g., "new", "processing", "completed"
}

export interface User {
  twitter_id: string;
  screen_name: string;
  name: string;
  status?: string; // Optional: e.g., "new", "processing", "completed"
}

export interface TwitterAccountFollowing {
  _id: ObjectId;
  user_id: string;
  following_id: string;
}
export interface User {
  twitter_id: string;
}

export interface Tracking {
  twitter_id: string;
}
export interface RawDataTwitterUserFriends {
  _id: ObjectId;
  user_id: string;
  screen_name: string;
  data: any[];
}
