import { Prisma } from '@prisma/client';
import { TransactionClient } from './common.interface';
const mbtis = [
  'INFP',
  'ENFP',
  'ISFP',
  'INFJ',
  'INTP',
  'ISTP',
  'ISFJ',
  'ENFJ',
  'ENTP',
  'ESFP',
  'INTJ',
  'ISTJ',
  'ESFJ',
  'ENTJ',
  'ESTP',
  'ESTJ',
];

const hobbys = [
  '여행',
  '언어교환',
  '대인관계',
  '어학',
  '독서',
  '요리',
  '고민상담',
  '쇼핑',
  '드라이브',
  '스트레스',
  '베이킹',
  'DIY',
  '사주',
];

const foods = [
  '맛집',
  '커피',
  '디저트',
  '스시',
  '술',
  '치킨',
  '떡볶이',
  '맥주',
  '파스타',
  '빵',
  '햄버거',
  '샐러드',
  '와인',
  '위스키',
];

const arts = [
  '음악',
  '사진',
  '패션',
  '그림 그리기',
  '메이크업',
  '뷰티',
  '전시회관람',
  '디자인',
  '필름카메라',
  '인테리어',
  '폴라로이드',
];

const sports = [
  '헬스',
  '댄스',
  '농구',
  '배구',
  '러닝',
  '수영',
  '야구',
  '자전거 타기',
  '등산',
  '볼링',
  '요가',
  '필라테스',
  '주짓수',
  '골프',
  '서핑',
  '스케이트보드',
  '스키',
  '클라이밍',
  '크로스핏',
];

const tags: Prisma.TagCreateInput[] = [
  ...mbtis,
  ...hobbys,
  ...foods,
  ...arts,
  ...sports,
].map((name) => ({ name }));

export async function tagSeed(client: TransactionClient) {
  await Promise.all(
    tags.map((tag) =>
      client.tag.create({
        data: tag,
      }),
    ),
  );
}