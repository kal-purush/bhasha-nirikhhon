import { LocalDate } from "@js-joda/core";
import { BookController } from "src/book/BookController";
import { BookService } from "src/book/BookService";
import { BookInfoDto } from "src/book/dto/BookInfoDto";
import { BookListResponseDto } from "src/book/dto/BookListResponseDto";
import { GetBooksQueryDto } from "src/book/dto/GetBooksQueryDto";
import { instance, mock, when } from "ts-mockito";

describe('BookController', () => {

  it('getBooks를 호출하면 BookService가 ResponseBookDto 오브젝트를 반환한다.', async () => {
    // given
    const bookInfoDtos: BookInfoDto[] = [
      new BookInfoDto(
        'greeng00se 단편선', 
        'https:\/\/fake.example.com\/asdf\/123\/456\/12345678.jpg?type=m1&udate=amavvdko',
        'qilip',
        '곤운',
        LocalDate.of(2016, 1, 1),
        '9791141844822'
      ),
    ]
    const bookListResponseDto: BookListResponseDto = new BookListResponseDto(1, bookInfoDtos);
    
    const mockService: BookService = mock(BookService);
    when(mockService.getBooks('qilip')).thenResolve(bookListResponseDto);
    
    const sut = new BookController(instance(mockService));

    // when
    const getBooksQueryDto: GetBooksQueryDto = { bookQuery: 'qilip' };
    const result = await sut.getBooks(getBooksQueryDto);

    // then
    expect(result).toBe(bookListResponseDto);
  })
})