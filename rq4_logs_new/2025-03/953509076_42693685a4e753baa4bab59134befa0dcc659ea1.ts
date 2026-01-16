import { http, HttpResponse } from 'msw';
import categoriesData from './categories.json';
import consultFaqsData from './consult_faqs.json';
import usageFaqsData from './usage_faqs.json';
import {
  Category,
  ConsultFaqCategory,
  UsageFaqCategory,
  Faq,
  PageInfo,
  FaqResponse,
} from '@/types';

const baseUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000';

const { consultCategories, usageCategories } = categoriesData;
const consultFaqs = consultFaqsData as Record<ConsultFaqCategory, Faq[]>;
const usageFaqs = usageFaqsData as Record<UsageFaqCategory, Faq[]>;

/**
 * 페이지네이션 정보 생성 헬퍼 함수
 * @param totalItems 총 아이템 수
 * @param offset 현재 페이지 오프셋
 * @param limit 페이지당 아이템 수
 * @returns 페이지네이션 정보
 */
const createPageInfo = (
  totalItems: Faq[],
  offset: number,
  limit: number,
): PageInfo => {
  return {
    totalRecord: totalItems.length,
    offset,
    limit,
    prevOffset: Math.max(0, offset - limit),
    nextOffset: offset + limit < totalItems.length ? offset + limit : offset,
  };
};

/**
 * 페이지네이션된 API 응답 생성 헬퍼 함수
 * @param items 아이템 배열
 * @param offset 현재 페이지 오프셋
 * @param limit 페이지당 아이템 수
 * @returns 페이지네이션된 API 응답
 */
const createResponse = (
  items: Faq[],
  offset: number,
  limit: number,
): FaqResponse => {
  return {
    pageInfo: createPageInfo(items, offset, limit),
    items: items.slice(offset, offset + limit),
  };
};

export const handlers = [
  http.get(`${baseUrl}/api/faq/category`, async ({ request }) => {
    const url = new URL(request.url);
    const tab = url.searchParams.get('tab') as Category | null;

    if (tab === 'CONSULT') {
      return HttpResponse.json(consultCategories);
    }

    if (tab === 'USAGE') {
      return HttpResponse.json(usageCategories);
    }

    return new HttpResponse(null, { status: 400 });
  }),

  http.get(`${baseUrl}/api/faq`, async ({ request }) => {
    const url = new URL(request.url);
    const limit = Number(url.searchParams.get('limit') || '10');
    const offset = Number(url.searchParams.get('offset') || '0');
    const tab = url.searchParams.get('tab') as Category | null;
    const faqCategoryID = url.searchParams.get('faqCategoryID');

    if (tab === 'CONSULT') {
      if (!faqCategoryID) {
        const allConsultFaqs = Object.values(consultFaqs).flat();
        return HttpResponse.json(createResponse(allConsultFaqs, offset, limit));
      }

      const categoryId = faqCategoryID as ConsultFaqCategory;
      if (categoryId in consultFaqs) {
        const faqs = consultFaqs[categoryId];
        return HttpResponse.json(createResponse(faqs, offset, limit));
      }

      return HttpResponse.json(createResponse([], offset, limit));
    }

    if (tab === 'USAGE') {
      if (!faqCategoryID) {
        const allUsageFaqs = Object.values(usageFaqs).flat();
        return HttpResponse.json(createResponse(allUsageFaqs, offset, limit));
      }

      const categoryId = faqCategoryID as UsageFaqCategory;
      if (categoryId in usageFaqs) {
        const faqs = usageFaqs[categoryId];
        return HttpResponse.json(createResponse(faqs, offset, limit));
      }

      return HttpResponse.json(createResponse([], offset, limit));
    }

    return HttpResponse.json(createResponse([], offset, limit));
  }),
];