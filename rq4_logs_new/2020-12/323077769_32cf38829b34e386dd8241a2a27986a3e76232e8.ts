/**
 * 将File（Blob）对象转变为一个dataURL字符串
 *
 * @param {ImageData} data
 * @returns {number[]]} []
 */
export default function imageDataToRGB(data: [], width: number, height: number): number[] {
  let length = width * height * 4;
  let i = 0;
  let rgb: number[] = [];

  while (i < length) {
      rgb.push(data[i++]);
      rgb.push(data[i++]);
      rgb.push(data[i++]);
      i++; // for the alpha channel which we don't care about
  }

  return rgb;
}