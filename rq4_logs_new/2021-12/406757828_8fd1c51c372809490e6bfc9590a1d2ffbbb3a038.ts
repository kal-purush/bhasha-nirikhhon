import { Rule } from '@sanity/types';
import SearchPageLinkPreview from '../components/SearchPageLinkPreview';

export default {
  title: '検索ページリンク',
  name: 'searchPageLink',
  type: 'object',
  fields: [
    {
      title: 'タイトル',
      name: 'title',
      type: 'string',
      validation: (Rule: Rule) => Rule.required(),
    },
    {
      title: '検索クエリ',
      name: 'query',
      type: 'string',
    },
    {
      title: 'カテゴリー',
      name: 'category',
      type: 'category',
    },
  ],
  preview: {
    select: {
      title: 'title',
      query: 'query',
      category: 'category',
    },
    component: SearchPageLinkPreview,
  },
};