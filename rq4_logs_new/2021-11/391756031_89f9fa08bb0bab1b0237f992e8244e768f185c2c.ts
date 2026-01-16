const db = {
  statuses: [
    {
      created_at: 'Fri Oct 22 09:06:02 +0000 2021',
      /**
       * IDの取り扱いに関して、Bigintに相当する大きさのものは、JSONとして出力する際にTypeErrorが発生する
       * 対応策として、予め文字列として定義している
       *
       * @see https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Global_Objects/BigInt#json_%E3%81%A7%E3%81%AE%E4%BD%BF%E7%94%A8
       */
      id: '1451474917262704641',
      id_str: '1451474917262704641',
      text: '半分こ、とか可愛いこと言ってすいませんでした！秒で一個平らげました☺️ https://t.co/kEK0v1uuRE',
      truncated: false,
      entities: {
        hashtags: [],
        symbols: [],
        user_mentions: [],
        urls: [],
        media: [
          {
            id: '1451474910723796994',
            id_str: '1451474910723796994',
            indices: [36, 59],
            media_url: 'http://pbs.twimg.com/media/FCSsvbYVcAINZUC.jpg',
            media_url_https: 'https://pbs.twimg.com/media/FCSsvbYVcAINZUC.jpg',
            url: 'https://t.co/kEK0v1uuRE',
            display_url: 'pic.twitter.com/kEK0v1uuRE',
            expanded_url:
              'https://twitter.com/TwitterJP/status/1451474917262704641/photo/1',
            type: 'photo',
            sizes: {
              small: {
                w: 510,
                h: 680,
                resize: 'fit',
              },
              thumb: {
                w: 150,
                h: 150,
                resize: 'crop',
              },
              large: {
                w: 1536,
                h: 2048,
                resize: 'fit',
              },
              medium: {
                w: 900,
                h: 1200,
                resize: 'fit',
              },
            },
          },
        ],
      },
      extended_entities: {
        media: [
          {
            id: '1451474910723796994',
            id_str: '1451474910723796994',
            indices: [36, 59],
            media_url: 'http://pbs.twimg.com/media/FCSsvbYVcAINZUC.jpg',
            media_url_https: 'https://pbs.twimg.com/media/FCSsvbYVcAINZUC.jpg',
            url: 'https://t.co/kEK0v1uuRE',
            display_url: 'pic.twitter.com/kEK0v1uuRE',
            expanded_url:
              'https://twitter.com/TwitterJP/status/1451474917262704641/photo/1',
            type: 'photo',
            sizes: {
              small: {
                w: 510,
                h: 680,
                resize: 'fit',
              },
              thumb: {
                w: 150,
                h: 150,
                resize: 'crop',
              },
              large: {
                w: 1536,
                h: 2048,
                resize: 'fit',
              },
              medium: {
                w: 900,
                h: 1200,
                resize: 'fit',
              },
            },
          },
        ],
      },
      metadata: {
        iso_language_code: 'ja',
        result_type: 'recent',
      },
      source:
        '<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>',
      in_reply_to_status_id: '1450758553732534272',
      in_reply_to_status_id_str: '1450758553732534272',
      in_reply_to_user_id: '7080152',
      in_reply_to_user_id_str: '7080152',
      in_reply_to_screen_name: 'TwitterJP',
      user: {
        id: '7080152',
        id_str: '7080152',
        name: 'Twitter Japan',
        screen_name: 'TwitterJP',
        location: '東京都中央区',
        description: 'What is Happening?! 今日も何かが起きている?!',
        url: 'https://t.co/IgW6OEkIbG',
        entities: {
          url: {
            urls: [
              {
                url: 'https://t.co/IgW6OEkIbG',
                expanded_url: 'https://blog.twitter.com/ja_jp.html',
                display_url: 'blog.twitter.com/ja_jp.html',
                indices: [0, 23],
              },
            ],
          },
          description: {
            urls: [],
          },
        },
        protected: false,
        followers_count: 2279550,
        friends_count: 108,
        listed_count: 14934,
        created_at: 'Tue Jun 26 01:54:35 +0000 2007',
        favourites_count: 1054,
        utc_offset: null,
        time_zone: null,
        geo_enabled: true,
        verified: true,
        statuses_count: 14493,
        lang: null,
        contributors_enabled: false,
        is_translator: false,
        is_translation_enabled: false,
        profile_background_color: 'C0DEED',
        profile_background_image_url:
          'http://abs.twimg.com/images/themes/theme1/bg.png',
        profile_background_image_url_https:
          'https://abs.twimg.com/images/themes/theme1/bg.png',
        profile_background_tile: true,
        profile_image_url:
          'http://pbs.twimg.com/profile_images/1354485696392613893/9T6ogVOI_normal.jpg',
        profile_image_url_https:
          'https://pbs.twimg.com/profile_images/1354485696392613893/9T6ogVOI_normal.jpg',
        profile_banner_url:
          'https://pbs.twimg.com/profile_banners/7080152/1633310159',
        profile_link_color: '1B95E0',
        profile_sidebar_border_color: 'FFFFFF',
        profile_sidebar_fill_color: 'DDEEF6',
        profile_text_color: '333333',
        profile_use_background_image: true,
        has_extended_profile: true,
        default_profile: false,
        default_profile_image: false,
        following: null,
        follow_request_sent: null,
        notifications: null,
        translator_type: 'regular',
        withheld_in_countries: [],
      },
      geo: null,
      coordinates: null,
      place: null,
      contributors: null,
      is_quote_status: false,
      retweet_count: 21,
      favorite_count: 263,
      favorited: false,
      retweeted: false,
      possibly_sensitive: false,
      lang: 'ja',
    },
  ],
  search_metadata: {
    completed_in: 0.017,
    max_id: '1451474917262704641',
    max_id_str: '1451474917262704641',
    next_results:
      '?max_id=1451474917262704640&q=exclude%3Aretweets%20filter%3Aimages%20from%3ATwitterJP&count=100&include_entities=1',
    query: 'exclude%3Aretweets+filter%3Aimages+from%3ATwitterJP',
    refresh_url:
      '?since_id=1451474917262704641&q=exclude%3Aretweets%20filter%3Aimages%20from%3ATwitterJP&include_entities=1',
    count: 100,
    since_id: 0,
    since_id_str: '0',
  },
}

export { db }