import { Module } from '@nestjs/common';
import { VideoCategoryController } from './video_category.controller';
import { VideoCategoryService } from './video_category.service';
import { VideoCategoryRepository } from './video_category.repository';
import { MongooseModule } from '@nestjs/mongoose';
import { VideoCategory, VideoCategorySchema } from './video_category.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: VideoCategory.name, schema: VideoCategorySchema },
    ]),
  ],
  controllers: [VideoCategoryController],
  providers: [VideoCategoryService, VideoCategoryRepository],
})
export class VideoCategoryModule {}