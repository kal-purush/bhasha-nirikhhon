export type Prefecture = {
  name: string;
  romaji: string;
};

export type Region = {
  name: string;
  prefectures: Prefecture[];
};

export const JAPAN_REGIONS: Region[] = [
  {
    name: '北海道・東北',
    prefectures: [
      { name: '北海道', romaji: 'hokkaido' },
      { name: '青森県', romaji: 'aomori' },
      { name: '岩手県', romaji: 'iwate' },
      { name: '宮城県', romaji: 'miyagi' },
      { name: '秋田県', romaji: 'akita' },
      { name: '山形県', romaji: 'yamagata' },
      { name: '福島県', romaji: 'fukushima' },
    ],
  },
  {
    name: '関東',
    prefectures: [
      { name: '茨城県', romaji: 'ibaraki' },
      { name: '栃木県', romaji: 'tochigi' },
      { name: '群馬県', romaji: 'gunma' },
      { name: '埼玉県', romaji: 'saitama' },
      { name: '千葉県', romaji: 'chiba' },
      { name: '東京都', romaji: 'tokyo' },
      { name: '神奈川県', romaji: 'kanagawa' },
    ],
  },
  {
    name: '中部',
    prefectures: [
      { name: '新潟県', romaji: 'niigata' },
      { name: '富山県', romaji: 'toyama' },
      { name: '石川県', romaji: 'ishikawa' },
      { name: '福井県', romaji: 'fukui' },
      { name: '山梨県', romaji: 'yamanashi' },
      { name: '長野県', romaji: 'nagano' },
      { name: '岐阜県', romaji: 'gifu' },
      { name: '静岡県', romaji: 'shizuoka' },
      { name: '愛知県', romaji: 'aichi' },
    ],
  },
  {
    name: '近畿',
    prefectures: [
      { name: '三重県', romaji: 'mie' },
      { name: '滋賀県', romaji: 'shiga' },
      { name: '京都府', romaji: 'kyoto' },
      { name: '大阪府', romaji: 'osaka' },
      { name: '兵庫県', romaji: 'hyogo' },
      { name: '奈良県', romaji: 'nara' },
      { name: '和歌山県', romaji: 'wakayama' },
    ],
  },
  {
    name: '中国',
    prefectures: [
      { name: '鳥取県', romaji: 'tottori' },
      { name: '島根県', romaji: 'shimane' },
      { name: '岡山県', romaji: 'okayama' },
      { name: '広島県', romaji: 'hiroshima' },
      { name: '山口県', romaji: 'yamaguchi' },
    ],
  },
  {
    name: '四国',
    prefectures: [
      { name: '徳島県', romaji: 'tokushima' },
      { name: '香川県', romaji: 'kagawa' },
      { name: '愛媛県', romaji: 'ehime' },
      { name: '高知県', romaji: 'kochi' },
    ],
  },
  {
    name: '九州・沖縄',
    prefectures: [
      { name: '福岡県', romaji: 'fukuoka' },
      { name: '佐賀県', romaji: 'saga' },
      { name: '長崎県', romaji: 'nagasaki' },
      { name: '熊本県', romaji: 'kumamoto' },
      { name: '大分県', romaji: 'oita' },
      { name: '宮崎県', romaji: 'miyazaki' },
      { name: '鹿児島県', romaji: 'kagoshima' },
      { name: '沖縄県', romaji: 'okinawa' },
    ],
  },
];