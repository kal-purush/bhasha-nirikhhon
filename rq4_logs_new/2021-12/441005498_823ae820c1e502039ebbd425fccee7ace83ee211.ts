import { Module } from '@nestjs/common';
import { CatsController } from './cats.controller';
import { CatsService } from './cats.service';
import { CatSchema } from './cats.schema';
import { MongooseModule } from '@nestjs/mongoose';
import { DatabaseModule } from 'src/database/database.module';
import { catsProviders } from './cats.providers';

@Module({
  imports: [
    DatabaseModule,
    // the name here is what the table name will become in the database
    MongooseModule.forFeature([{ name: 'CATS', schema: CatSchema }]),
  ],
  controllers: [CatsController],
  providers: [CatsService, ...catsProviders],
})
export class CatsModule {}