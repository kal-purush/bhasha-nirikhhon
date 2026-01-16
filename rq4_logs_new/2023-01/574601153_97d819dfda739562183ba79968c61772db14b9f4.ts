import { Module } from '@nestjs/common';

import { TasksService } from './tasks.service';

import { DcInsideCrawlerModule } from '@app/crawler/dc-inside-crawler/dc-inside-crawler.module';
import { RedditCrawlerModule } from '@app/crawler/reddit-crawler/reddit-crawler.module';
import { TwitterCrawlerModule } from '@app/crawler/twitter-crawler/twitter-crawler.module';
import { YoutubeCrawlerModule } from '@app/crawler/youtube-crawler/youtube-crawler.module';

@Module({
  imports: [
    RedditCrawlerModule,
    DcInsideCrawlerModule,
    YoutubeCrawlerModule,
    TwitterCrawlerModule,
  ],
  providers: [TasksService],
})
export class TasksModule {}