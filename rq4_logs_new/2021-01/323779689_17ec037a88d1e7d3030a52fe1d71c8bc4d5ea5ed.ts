import { readdirSync, readFileSync } from 'fs-extra';
import yaml from 'js-yaml';
import { join } from 'path';

interface Page {
    // 标题信息
    title: string
    // 路径信息
    path: string
}

interface Group {
    // 标题
    title: string
    // 菜单路径
    path: string
    // 当前的页面信息
    pages: Page[]
}

interface Nav {
    // 标题
    title: string
    // 菜单路径
    path: string
    // 分组信息
    groups: Group[]
}

// 查找对应的md的文件, 然后根据文件来生成对应的目录结构信息
export const findFile = () => {
    const dir = readdirSync(process.cwd());
    const mdFiles = dir.filter((file) => file.match(/\.md(x?)$/));
    return mdFiles;
};

// 找到当前的路由信息，并生成navs信息
export const findFileToNavs = () => {
    const files = findFile();
    const navs: Nav[] = [];
    files.forEach((file) => {
        const mdInfo = /^---[\W\w]+?---/i.exec(readFileSync(file, 'utf8'));
        if (mdInfo?.length === 1) {
            /**
             * 数据格式如下
             * nav:
             *  title: 组件
             *  path: /components
             * group:
             *  path: /components/base
             *  title: 基础组件
             * title: Button 按钮`;
             */
            const yml: any = yaml.safeLoad(mdInfo[0]);
            const filterNav = navs.filter((ele) => ele.path === yml.nav.path);
            // 不存在对应的nav信息，则添加
            const groupInfo = {
                title: yml.group.title as string,
                path: yml.group.path as string,
                pages: [{
                    title: yml.title as string,
                    path: file.replace(process.cwd(), ''),
                }],
            };
            if (navs.filter((ele) => ele.path === yml.nav.path).length === 0) {
                navs.push({
                    title: yml.nav.title as string,
                    path: yml.nav.path as string,
                    groups: [groupInfo],
                });
            } else {
                const groupFilter = filterNav[0].groups.filter((group) => group.path === yml.group.path);
                if (groupFilter.length === 0) {
                    groupFilter.push(groupInfo);
                } else {
                    groupFilter[0].pages.push(groupInfo.pages[0]);
                }
            }
        }
    });
    return navs;
};