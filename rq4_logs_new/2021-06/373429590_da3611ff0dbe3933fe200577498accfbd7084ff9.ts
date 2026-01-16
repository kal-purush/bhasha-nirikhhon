import { Controller, Get, Post, Put, Delete, Param, Body } from '@nestjs/common';
import { BooksService } from './books.service';
import { BookDTO } from './dto/book.dto';
import { Book } from './interface/book.interface';

@Controller('books')
export class BooksController {
  constructor(private readonly booksService: BooksService) {}

  @Get()
  findAll(): Promise<Book[]> {
    return this.booksService.findAll();
  }

  @Get(':id')
  find(@Param('id') id): Promise<Book> {
    return this.booksService.find(id);
  }

  @Post()
  create(@Body() book: BookDTO): Promise<Book> {
    console.log("book>>", book)
    return this.booksService.create(book);
  }

  @Put(':id')
  update(@Param('id') id, @Body() book: BookDTO): Promise<Book> {
    return this.booksService.update(id, book);
  }

  @Delete(':id')
  delete(@Param('id') id): Promise<Book> {
    return this.booksService.delete(id);
  }
}