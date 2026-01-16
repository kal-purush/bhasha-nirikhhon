// B 站视频请求返回类型
export interface VideoInfo {
  arcrank: string;
  arcurl: string;
  author: string;
  badgepay: boolean;
  bvid: string; // bv 号
  description: string;
  duration: number; // 时长/秒
  favorites: number; // 收藏数
  id: number;
  is_pay: number;
  is_union_video: number;
  mid: number;
  pic: string; // 预览图
  play: string; // 播放量
  pubdate: string;
  rank_offset: number;
  rank_index: number;
  rank_score: number;
  review: number;
  senddate: number;
  tag: string;
  title: string;
  type: string;
  video_review: number;
}