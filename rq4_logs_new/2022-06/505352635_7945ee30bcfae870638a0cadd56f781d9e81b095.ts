import { DateTimeFormatter, LocalDate } from "@js-joda/core";
import { Injectable } from "@nestjs/common";
import { BookInfoDto } from "../dto/book-info.dto";

@Injectable()
export class ApiResultCoverter {
  // API의 반환 값이 query와 일치하는 값을 HTML 태그로 감싸기 때문에 정규표현식을 이용하여 태그 제거
  private readonly REGEXP: RegExp = /<[^>]*>?/g;
  private readonly PATTERN: DateTimeFormatter = DateTimeFormatter.ofPattern('yyyyMMdd');
  private readonly PADDING = '19000101';

  public toBookInfoDtos(searchResult: any): BookInfoDto[] {
    const bookInfoDtos: BookInfoDto[] = searchResult.data.items.map(bookInfo =>
      new BookInfoDto(
        bookInfo.title.replace(this.REGEXP, ''),
        bookInfo.image,
        // API의 author 반환 값이 공동저자인 경우 '|' 로 구분하는 것을 ', ' 로 구분하도록 변경
        bookInfo.author.replace(this.REGEXP, '').replaceAll('|', ', '),
        bookInfo.publisher.replace(this.REGEXP, ''),
        this.stringToLocalDate(bookInfo.pubdate),
        // API의 ISBN 반환값이 '<구ISBN> <신ISBN>' 형식이므로 신ISBN 13자리만 추출하여 사용
        bookInfo.isbn.replace(this.REGEXP, '').split(' ')[1]
      )
    );
    return bookInfoDtos;
  }

  private stringToLocalDate(pubdate: string): LocalDate {
    const localDate: LocalDate = LocalDate.parse(
      // API의 pubdate 반환 값이 4자리인 경우가 존재함으로 패딩 추가
      pubdate.padEnd(this.PADDING.length, this.PADDING), 
      this.PATTERN
    );
    return localDate;
  }
}