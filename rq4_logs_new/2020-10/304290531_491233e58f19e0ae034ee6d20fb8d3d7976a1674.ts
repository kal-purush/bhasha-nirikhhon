import axios from 'axios';
import { API_URL } from '@/utils/config';

/** 推送视频后端返回 */
export interface PushRecord {
  /** 用户 Id */
  uid: string;
  /** BV 号 */
  bv: string;
  /** 作者 */
  author: string;
  /** 预览图片 */
  pic: string;
  /** 更新时间 */
  pubdate: string;
  /** 标题 */
  title: string;
  /** 描述 */
  description: string;
}

const getRecords: () => Promise<PushRecord[]> = () =>
  new Promise<PushRecord[]>((resolve, reject) => {
    axios
      .get(`${API_URL}/records`)
      .then((resp) => {
        resolve(resp.data);
      })
      .catch((err) => reject(err));
  });

const recordClient = {
  getRecords,
};

export default recordClient;