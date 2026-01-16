import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { MediaSchema } from './schemas/media.schemas';
import { MediaController } from './medias.controller';
import { MediaService } from './medias.service';

@Module({
  imports: [MongooseModule.forFeature([{ name: 'Media', schema: MediaSchema }])],
  controllers: [MediaController],
  providers: [MediaService],
  exports: [MediaService],
})
export class MediaModule {}