/**
 * 从 B 站拉取视频
 */
import { VideoInfo } from '@/types/video';
import logger from '@/utils/logger';

const keyword = 'VLOG';
const page = 400;

const url = `https://s.search.bilibili.com/cate/search?main_ver=v3&search_type=video&view_type=hot_rank&order=click&copy_right=-1&cate_id=21&page=${page}&pagesize=20&keyword=${keyword}`;

// 获取一个视频的信息
export default async function fetchVideo(): Promise<VideoInfo> {
  const response = await fetch(url);
  const result = await response.json(); // TODO: 类型
  logger.info(result);
  return result.result[0];
}