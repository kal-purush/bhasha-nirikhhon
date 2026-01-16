/**
 * 获取url后指定属性的值
 */
function getKeyValueArr(url) {
    let value = url.match(/(?<=elective=)(\d+(\,\d+)*)/)
    return value ? value[0].split(',') : []
}
const URL_EMPTY_VALUE = 'https://www.xx.cn/api?keyword=&level1=&local_batch_id=&elective=&local_province_id=33'
const URL_DOUBLE_VALUES = 'https://www.xx.cn/api?keyword=&level1=&local_batch_id=&elective=800,700&local_province_id=33'
console.log(getKeyValueArr(URL_EMPTY_VALUE))
console.log(getKeyValueArr(URL_DOUBLE_VALUES))