import { globSync } from 'glob';
import path from 'path';

const collectionMqp = {
  other: 'ot',
  menu: 'menu',
  'context-menu': 'cm',
};
export function getIcons() {
  const iconFiles = globSync('src/assets/icons/**/*.svg', { nodir: true });
  const resIcons = [];
  iconFiles.forEach((filePath) => {
    const fileName = path.basename(filePath); // 获取文件名，包括后缀
    const fileNameWithoutExt = path.parse(fileName).name; // 获取去除后缀的文件名
    const dirnames = path.dirname(filePath).split('\\');
    const collectionName = collectionMqp[dirnames[dirnames.length - 1]];
    resIcons.push(`i-${collectionName}-${fileNameWithoutExt}`);
  });

  return resIcons;
}