export interface TweetEntity {
  created_at: string;
  id: number;
  id_str: string; // https://twitter.com/i/web/status/ のここに入るやつ
  text: string;
  entities: {
    urls: {
      // b.hatena.ne.jp から始まるものははてなブックマーク経由、urlパラメータがない場合はリダイレクト
      // htn.to は記事へのリダイレクタ
      // その下に対象のTweetのid_strも載ってる場合がある（一緒なら無視していいが、違う場合は引用RTかな？）
      expanded_url: string;
    }[];
  };
  user: TweetUser;
  // 引用RTの場合、entitiesのurlsに対象の記事のURLが無いかもしれないので、こっちをまず優先してみる
  retweeted_status?: Omit<TweetEntity, "retweeted_status">;
}

interface TweetUser {
  id: number;
  screen_name: string;
  followers_count: number;
}

export interface StoreSummary {
  id: TweetEntity["id"];
  id_str: TweetEntity["id_str"];
  created_at: TweetEntity["created_at"];
  retweeted_id: TweetEntity["id"] | "";
  user_id: TweetUser["id"];
  screen_name: TweetUser["screen_name"];
  followers_count: number;
  entry: string;
  text: string;
}

export type SettledPromiseValue<T, U> =
  | { status: "fulfilled"; value: T }
  | { status: "rejected"; value: U };