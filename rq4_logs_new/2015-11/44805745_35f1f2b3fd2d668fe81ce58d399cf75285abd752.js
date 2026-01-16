import querystring from 'querystring';
import FormData from 'form-data';

// 后海大道，南山
export function createParamObj(address, area) {
  return {
    parameters: [
      { name: 'address', value: address },
      { name: 'areaid', value: '440305000000000000', flag: 16, group: 'system.areaid.client' },
      { name: 'areaid', value: '440305999999999999', flag: 17, group: 'system.areaid.client' }
    ],
    pageNum: 1,
    pageSize: 12,
    name: 'viewcodebuildingweb',
    getCodeValue: true,
    toCount: false,
    isgis: false,
    javaClass: 'com.longrise.LEAP.BLL.Cache.SearchBuilder'
  };
}

export function paramToStr(paramObj) {
  return JSON.stringify([JSON.stringify(paramObj)]);
}

export function encodeFormData(str) {
  return new Buffer(encodeURIComponent(escape(str))).toString('base64');
}

export function getBody(address, area) {
  // const form = new FormData();
  // form.append('a', encodeFormData(paramToStr(createParamObj(address, area))));
  // return form;
  return { a: encodeFormData(paramToStr(createParamObj(address, area))) };
}

export function decodeFormData(base64Str) {
  return unescape(decodeURIComponent(new Buffer(base64Str, 'base64').toString()));
}

export const API_URL = 'http://www.szzlb.gov.cn/LEAPV5/LEAP/Service/RPC/RPC.DO';

export function getUrl(method, sid) {
  return API_URL + '?' + createQuerystring(method, sid) + '&rup=http://www.szzlb.gov.cn/LEAPV5/&_website_=*';
}

/**
 * @param {string} method
 *   app_getLogicModuleOperations 获取深圳各个行政区的areaid
 *   getCodeValuesByParentValue  获取行政区下属街道
 *   beanSearch  - 查询房屋编码
 * @param {string} sid - session ID (D6E4E9282D87A0CD68FF115670DF261A) 应该会过期
 */
export function createQuerystring(method, sid) {
  const rst = {
    callService: 'web',
    method,
    sid,
  };
  return querystring.stringify(rst);
}

export function getHeaders() {
  return {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Data-Type': 4,
    'Host': 'www.szzlb.gov.cn',
    'Origin': 'http://www.szzlb.gov.cn',
    'Post-Type': 1,
    'Pragma': 'no-cache',
    'Referer': 'http://www.szzlb.gov.cn/LEAPV5/LEAP/UnitModule/czww_search/buildingsearch.html',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36',
  };
}