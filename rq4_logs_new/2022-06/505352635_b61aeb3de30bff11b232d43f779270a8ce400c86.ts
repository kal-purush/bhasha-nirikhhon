import { LocalDate } from "@js-joda/core";
import { BadRequestException, INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import 'dotenv/config';
import { BooksModule } from "src/book/BookModule";
import { BookRepository } from "src/book/BookRepository";
import { BookSearchService } from 'src/book/BookSearchService';
import { BookInfoDto } from "src/book/dto/BookInfoDto";
import { BookListResponseDto } from "src/book/dto/BookListResponseDto";
import * as request from 'supertest';
import { instance, mock, when } from "ts-mockito";

describe('BookController (e2e)', () => {
  let app: INestApplication;

  const bookInfoDtos: BookInfoDto[] = [
    new BookInfoDto(
      '큐피와 그린구스의 모험',
      'https:\/\/fake.example.com\/asdf\/123\/456\/12345678.jpg?type=m1&udate=amavvdko',
      'greeng00se',
      '곤운 출판사',
      LocalDate.of(2020, 10, 10),
      '1111141844822'
    ),
  ]
  const bookListResponseDto: BookListResponseDto = new BookListResponseDto(1, bookInfoDtos);

  beforeAll(async () => {
    const mockSearchService: BookSearchService = mock(BookSearchService);
    const mockRepository: BookRepository = mock(BookRepository);
    when(mockSearchService.callBookApi('큐피와 그린구스')).thenResolve(bookListResponseDto);
    when(mockSearchService.callBookApi(`''`)).thenThrow(new BadRequestException(["bookQuery should not be empty"]));
    const moduleRef = await Test.createTestingModule({ imports: [BooksModule] })
      .overrideProvider(BookSearchService).useValue(instance(mockSearchService))
      .overrideProvider(BookRepository).useValue(instance(mockRepository))
      .compile();

    app = moduleRef.createNestApplication();
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  it("/GET /books?bookQuery='' 요청시 400 Bad Request 반환", async () => {
    const response = await request(app.getHttpServer())
      .get("/books?bookQuery=''")
      .expect(400);
    expect(response.body.message).toStrictEqual(['bookQuery should not be empty']);
    expect(response.body.error).toBe('Bad Request');
  });

  it("/GET /boks?bookQuery=김케장 요청시 책 목록과 200 OK 반환", async () => {

    const response = await request(app.getHttpServer())
      .get("/books?bookQuery="+encodeURI('큐피와 그린구스'))
      .expect(200);
    expect(response.body.items[0].title).toBe('큐피와 그린구스의 모험');
  });
});