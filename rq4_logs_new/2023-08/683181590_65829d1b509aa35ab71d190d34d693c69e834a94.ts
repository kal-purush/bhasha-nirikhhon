import { promises } from "dns";

export class VnExpressRss {
    static async init(data: any[]): Promise<any> {
        let result: any[] = [];
        data.forEach((element: any) => {
            let news: any = {};

            let content = element.content;
            let myImage = content.match('src\*s*=\s*"([^"]+)"');
            let myContent = content.match('.*br>(.*)');

            news.title = element.title;
            news.link = element.link;
            news.pubDate = element.pubDate;
            news.content = myContent[1] || '';
            news.image = myImage[1] || '';

            result.push(news);
        });

        return result;
    }
}