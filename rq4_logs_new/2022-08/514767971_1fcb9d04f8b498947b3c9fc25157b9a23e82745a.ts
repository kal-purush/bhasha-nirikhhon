/* eslint-disable */

const jsdom = require('jsdom');
const { JSDOM } = jsdom;

const createBlogCardData = async (content: string) => {
  //ブログカード生成
  const lines = content.split('\n');
  const links: string[] | never = [];
  // URLの取得
  lines.map((line) => {
    if (line.indexOf('http://') === 0) links.push(line);
    if (line.indexOf('https://') === 0) links.push(line);
  });
  let cardData = [];

  const temps = await Promise.all(
    links.map(async (link) => {
      const metas = await fetch(link)
        .then((res) => res.text())
        .then((text) => {
          const metaData = {
            url: link,
            title: '',
            description: '',
            image: '',
          };
          const doms = new JSDOM(text);
          const metas: any = doms.window.document.getElementsByTagName('meta');

          // title, description, imageにあたる情報を取り出し配列として格納
          for (let i = 0; i < metas.length; i++) {
            let pro = metas[i].getAttribute('property');
            if (typeof pro == 'string') {
              if (pro.match('title')) metaData.title = metas[i].getAttribute('content');
              if (pro.match('description')) metaData.description = metas[i].getAttribute('content');
              if (pro.match('image')) metaData.image = metas[i].getAttribute('content');
            }
            pro = metas[i].getAttribute('name');
            if (typeof pro == 'string') {
              if (pro.match('title')) metaData.title = metas[i].getAttribute('content');
              if (pro.match('description')) metaData.description = metas[i].getAttribute('content');
              if (pro.match('image')) metaData.image = metas[i].getAttribute('content');
            }
          }
          return metaData;
        })
        .catch((e) => {
          console.log(e);
        });
      return metas;
    }),
  );
  return (cardData = temps.filter((temp) => temp !== undefined));
};

export default createBlogCardData;