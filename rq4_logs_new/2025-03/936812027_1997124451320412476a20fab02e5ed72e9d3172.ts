import { defineField, defineType } from 'sanity';

export const blogType = defineType({
  name: 'blog',
  title: 'Blog',
  type: 'document',
  fields: [
    defineField({
      name: 'title',
      title: 'Title',
      type: 'string',
      validation: (rule) => rule.required(),
    }),
    defineField({
      name: 'slug',
      title: 'Slug',
      type: 'slug',
      options: {
        source: 'title',
        maxLength: 96,
      },
      validation: (rule) => rule.required(),
    }),
    defineField({
      name: 'description',
      title: 'Description',
      type: 'text',
    }),
    defineField({
      name: 'content',
      title: 'Content',
      type: 'array',
      of: [
        { type: 'block' },
        {
          type: 'code',
          name: 'code',
          title: 'Code',
          options: {
            languageAlternatives: [
              { title: '', value: '' },
              { title: 'JavaScript', value: 'javascript' },
              { title: 'TypeScript', value: 'typescript' },
              { title: 'HTML', value: 'html' },
              { title: 'CSS', value: 'css' },
              { title: 'React JSX', value: 'jsx' },
              { title: 'React TSX', value: 'tsx' },
              { title: 'JSON', value: 'json' },
              { title: 'Bash', value: 'bash' },
            ],
            withFilename: false,
          },
        },
        {
          type: 'youtube',
          title: 'Video',
        },
      ],
    }),
  ],
});