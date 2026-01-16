import {defineField, defineType} from 'sanity'

export default defineType({
  name: 'home',
  title: 'Головна',
  type: 'document',
  groups: [
    {
      name: 'sponsors',
      title: 'Спонсори',
    },
    {
      name: 'allNews',
      title: 'Новини',
    },
    {
      name: 'winner',
      title: 'Переможці',
    },
    {
      name: 'videos',
      title: 'Відео',
    },
  ],

  fields: [
    defineField({
      name: 'banner',
      title: 'Баннер',
      type: 'object',
      fields: [
        defineField({
          name: 'title',
          title: 'Заголовок',
          type: 'internationalizedArrayString',
        }),
        defineField({
          name: 'dateBegin',
          title: 'Дата Події',
          type: 'internationalizedArrayString',
        }),
      ],
    }),

    defineField({
      group: ['allNews'],
      name: 'allNews',
      title: 'Новини',
      type: 'array',
      of: [{type: 'news'}],
    }),
    defineField({
      name: 'quote',
      title: 'Змінити цитату',
      type: 'object',
      fields: [
        defineField({
          name: 'desc',
          title: 'Цитата',
          type: 'internationalizedArrayText',
        }),
        defineField({
          name: 'author',
          title: 'Автор',
          type: 'internationalizedArrayString',
        }),
      ],
    }),

    defineField({
      group: ['winner'],
      name: 'winner',
      title: 'Переможці',
      type: 'array',
      of: [{type: 'winners'}],
    }),

    defineField({
      group: ['videos'],
      name: 'videos',
      title: 'Відео',
      type: 'array',
      of: [{type: 'video'}],
    }),
    defineField({
      group: ['sponsors'],
      name: 'sponsors',
      title: 'Спонсори',
      type: 'array',
      of: [{type: 'sponsor'}],
    }),
  ],
})