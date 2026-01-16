import {FetchLatestRssItemsUseCase} from "../usecases/fetchLatestRssItems.usecase.ts";
import {RssItem as _RssItem} from "../../domain/entities/rssItem.entity.ts";
import {CONFIG} from "../../infrastructure/config/config.ts";

let lastArticleTitle = "";

export class RssSchedulerService {
  constructor(private readonly fetchLatestRssItemsUseCase: FetchLatestRssItemsUseCase) {
  }

  async scheduleFetchRssItems(): Promise<void> {
    const items = await this.fetchLatestRssItemsUseCase.execute(CONFIG.RSS_FEED_URL, 5);
    const newItems = items.filter((item) => item.title !== lastArticleTitle);

    if (newItems.length > 0) {
      console.log("New articles from Abobon:");
      for (const item of newItems) {
        console.log(`Title: ${item.title}`);
        console.log(`Link: ${item.link}`);
        console.log("---");
      }
      lastArticleTitle = newItems[0]?.title || "";
    }
  }

  async testFetchRssItems(): Promise<void> {
    const items = await this.fetchLatestRssItemsUseCase.execute(CONFIG.RSS_FEED_URL, 5);
    console.log("Latest 5 RSS Items:");
    for (const item of items) {
      console.log(`Title: ${item.title}`);
      console.log(`Link: ${item.link}`);
      console.log("---");
    }
  }
}