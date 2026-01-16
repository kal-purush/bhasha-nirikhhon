import { DateTimeFormatter, LocalDate } from "@js-joda/core";
import { HttpService } from "@nestjs/axios";
import { Injectable } from "@nestjs/common";
import { firstValueFrom } from "rxjs";
import { BookInfoDto } from "./dto/book-info.dto";
import { ResponseBookListDto } from "./dto/response-book-list.dto";

@Injectable()
export class BookSearchService {
  private readonly URL: string = 'https://openapi.naver.com/v1/search/book.json';
  private readonly API_KEY = {
    'X-Naver-Client-Id': process.env.BOOKAPI_ID,
    'X-Naver-Client-Secret': process.env.BOOKAPI_PW
  };
  
  //! API의 반환 값이 query와 일치하는 값을 HTML 태그로 감싸기 때문에 정규표현식을 이용하여 태그 제거
  private readonly REGEXP: RegExp = /<[^>]*>?/g;
  private readonly PATTERN: DateTimeFormatter = DateTimeFormatter.ofPattern('yyyyMMdd');
  private readonly PADDING = '19000101';

  constructor (
      private readonly httpService: HttpService,
  ) {}
    
  public async callBookApi(bookQuery: string): Promise<ResponseBookListDto> {
    const params = { query: bookQuery };
    
    const searchResult = await firstValueFrom(this.httpService.get(this.URL, { params, headers: this.API_KEY }));
    const bookInfoDtos = this.getBookInfoDtos(searchResult);
    const responseBookListDto = new ResponseBookListDto(bookInfoDtos.length, bookInfoDtos);
    return responseBookListDto;
  }

  private getBookInfoDtos(searchResult: any): BookInfoDto[] {
    const bookInfoDtos: BookInfoDto[] = searchResult.data.items.map(bookInfo => 
      new BookInfoDto(
        bookInfo.title.replace(this.REGEXP, ''),
        bookInfo.image,
        bookInfo.author.replace(this.REGEXP, ''),
        bookInfo.publisher.replace(this.REGEXP, ''),
        //! API의 pubdate 반환 값이 4자리인 경우가 존재함으로 패딩 추가
        LocalDate.parse(bookInfo.pubdate.padEnd(this.PADDING.length, this.PADDING), this.PATTERN),
        //! API의 ISBN 반환값이 '<구ISBN> <신ISBN>' 형식이므로 신ISBN 13자리만 추출하여 사용
        bookInfo.isbn.replace(this.REGEXP, '').split(' ')[1])
    );

    return bookInfoDtos;
  }
}