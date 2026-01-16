import log from '@/utils/logger';
// import { API_URL } from '@/utils/config';
import axios from 'axios';

/** 请求参数 */
interface TestReq {
  /** 用户 ID */
  uid: string;
}

/** 返回列表项 */
interface TestRespItem {
  /** 用户 ID */
  uid: string;
  /** BV 号 */
  bv: string;
}

/** 返回 */
type TestResp = TestRespItem[];

/** 接口函数 */
const test: (req: TestReq) => Promise<TestResp> = ({ uid }) =>
  new Promise<TestResp>((resolve, reject) => {
    log.info('testClient.test');
    // FIXME: 后端上跨域后应该用 .get(`${API_URL}/records/xxx`)
    axios
      .get(`/api/records/byUid/${uid}`)
      .then((resp) => {
        resolve(resp.data);
      })
      .catch((err) => reject(err));
  });

const testClient = {
  test,
};

export default testClient;