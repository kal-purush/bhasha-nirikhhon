export const gameCodeList = {
  rv: {
    url: '/reversi',
    en: 'Reversi',
    jp: 'オセロ',
  },
  ch: {
    url: '/chess',
    en: 'Chess',
    jp: 'チェス',
  },
  sg: {
    url: '/shogi',
    en: 'Shogi',
    jp: '将棋',
  },
  gg: {
    url: '/gobblet',
    en: 'Gobblet Gobblers',
    jp: 'ゴブレット ゴブラーズ',
  },
};

export type GameCode = 'rv' | 'ch' | 'sg' | 'gg';

export type UserStatus = {
  userName: string;
  type: 'guest' | 'player';
  lv: number | null;
};

/**  */
export type Side = 'black' | 'white';

/**  */
export type PieceSide = Side | 'none';

export const reversiInitialPieceList: PieceSide[][] = [
  ['none', 'none', 'none', 'none', 'none', 'none', 'none', 'none'],
  ['none', 'none', 'none', 'none', 'none', 'none', 'none', 'none'],
  ['none', 'none', 'none', 'none', 'none', 'none', 'none', 'none'],
  ['none', 'none', 'none', 'black', 'white', 'none', 'none', 'none'],
  ['none', 'none', 'none', 'white', 'black', 'none', 'none', 'none'],
  ['none', 'none', 'none', 'none', 'none', 'none', 'none', 'none'],
  ['none', 'none', 'none', 'none', 'none', 'none', 'none', 'none'],
  ['none', 'none', 'none', 'none', 'none', 'none', 'none', 'none'],
];

export type FieldKey =
  | 'p00'
  | 'p01'
  | 'p02'
  | 'p03'
  | 'p04'
  | 'p05'
  | 'p06'
  | 'p07'
  | 'p10'
  | 'p11'
  | 'p12'
  | 'p13'
  | 'p14'
  | 'p15'
  | 'p16'
  | 'p17'
  | 'p20'
  | 'p21'
  | 'p22'
  | 'p23'
  | 'p24'
  | 'p25'
  | 'p26'
  | 'p27'
  | 'p30'
  | 'p31'
  | 'p32'
  | 'p33'
  | 'p34'
  | 'p35'
  | 'p36'
  | 'p37'
  | 'p40'
  | 'p41'
  | 'p42'
  | 'p43'
  | 'p44'
  | 'p45'
  | 'p46'
  | 'p47'
  | 'p50'
  | 'p51'
  | 'p52'
  | 'p53'
  | 'p54'
  | 'p55'
  | 'p56'
  | 'p57'
  | 'p60'
  | 'p61'
  | 'p62'
  | 'p63'
  | 'p64'
  | 'p65'
  | 'p66'
  | 'p67'
  | 'p70'
  | 'p71'
  | 'p72'
  | 'p73'
  | 'p74'
  | 'p75'
  | 'p76'
  | 'p77';

/**  */
export type Field = { [k in FieldKey]: PieceSide };

/**  */
export type PieceInfo = {
  black: number;
  white: number;
  none: number;
};

/** */
export type FieldInfo = {
  field: Field;
  turnedPieceCount: number;
};

/** */
export type PieceCount = {
  black: number;
  white: number;
};