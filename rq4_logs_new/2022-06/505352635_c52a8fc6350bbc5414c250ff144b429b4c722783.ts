import { LocalDate } from "@js-joda/core";
import { BookInfoDto } from "src/book/dto/book-info.dto";
import { ApiResultCoverter } from "src/book/helper/api-result.converter";

describe('[ApiResultCoverter]', () => {
  const apiResultCoverter: ApiResultCoverter = new ApiResultCoverter();

  it('Naver Search API 반환 결과를 bookInfoDto 배열로 변환한다', () => {
    // given
    const naverSearchApiResult: any = {
      "total": 2,
      "start": 1,
      "display": 2,
      "items": [
        {
          "title": "<b>greeng00se 단편선<\/b>",
          "image": "https:\/\/fake.example.com\/asdf\/123\/456\/12345678.jpg?type=m1&udate=amavvdko",
          "author": "<b>qilip<\/b>",
          "price": "100000000",
          "discount": "9000000",
          "publisher": "곤운",
          "pubdate": "2016", // 연도 4자리만 들어오는 케이스가 존재
          "isbn": "1234567890 9791141844822",
        },
        {
          "title": "<b>qilip 단편선<\/b>",
          "image": "https:\/\/fake.example.com\/asdf\/123\/456\/12345678.jpg?type=m1&udate=fdsaf",
          "author": "<b>qilip<\/b>",
          "price": "vcmxlzvm",
          "discount": "9000000",
          "publisher": "곤운",
          "pubdate": "20191212",
          "isbn": "1234567890 9791141844821",
        }
      ]
    };

    const bookInfoDtos: BookInfoDto[] = [
      new BookInfoDto(
        'greeng00se 단편선', 
        'https:\/\/fake.example.com\/asdf\/123\/456\/12345678.jpg?type=m1&udate=amavvdko',
        'qilip',
        '곤운',
        LocalDate.of(2016, 1, 1),
        '9791141844822'
      ),
      new BookInfoDto(
        'qilip 단편선',
        'https:\/\/fake.example.com\/asdf\/123\/456\/12345678.jpg?type=m1&udate=fdsaf',
        'qilip',
        '곤운',
        LocalDate.of(2019, 12, 12),
        '9791141844821'
      )
    ];

    // when
    const convertResult: BookInfoDto[] = apiResultCoverter.toBookInfoDtos(naverSearchApiResult);
    
    // then
    expect(convertResult).toStrictEqual(bookInfoDtos);
  });
});