import { HttpModule } from '@nestjs/axios';
import { Module } from '@nestjs/common';
import { TypeOrmExModule } from 'src/db/TypeOrmExModule';
import { BookController } from './BookController';
import { BookRepository } from './BookRepository';
import { BookSearchService } from './BookSearchService';
import { BookService } from './BookService';
import { ApiResultTransformer } from './transformer/ApiResultTransformer';

@Module({
  imports: [
    TypeOrmExModule.forCustomRepository([BookRepository]),
    HttpModule
  ],
  controllers: [BookController],
  providers: [BookService, BookSearchService, ApiResultTransformer]
})
export class BooksModule {}