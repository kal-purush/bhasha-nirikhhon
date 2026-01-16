const { freeze } = Object;

/** 计时模块常量 */
export const TIMEKEEPING = freeze({
  /** 计时时长/ms */
  DURATION: 7000,
});

/** 视频记录模块常量 */
export const RECORD_VIDEO = freeze({
  /** 视频时长上限/秒 */
  DURATION_UPPER_LIMIT: 60 * 15,
  /** 视频播放比例下限 */
  PLAYED_TIME_PROPORTION_LOWER_LIMIT: 1 / 3,
  /** 视频记录个数下限 */
  VIDEO_COUNT_LOWER_LIMIT: 5,
});

/** 拉取视频模块常量 */
export const FETCH_VIDEO = freeze({
  /** 视频所在分区 */
  KEYWORD: 'VLOG',
  /** 视频播放量上限 */
  AMOUNT_OF_PLAY_UPPER_LIMIT: 0,
  /** 视频长度下限/秒 */
  VIDEO_DURATION_LOWER_LIMIT: 60,
  /** 拉取视频的起始页数 */
  START_PAGE: 400,
  /** 拉取视频的结束页数 */
  END_PAGE: 390,
});