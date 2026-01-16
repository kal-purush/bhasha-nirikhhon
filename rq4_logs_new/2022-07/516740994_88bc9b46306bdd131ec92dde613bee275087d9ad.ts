import { defineDocumentType, makeSource } from 'contentlayer/source-files';

const titleFilter = (props: string) =>
  props.replace(/ /g, '+').replace(/([^a-zA-z0-9가-힣-+])/g, '');

export const Post = defineDocumentType(() => ({
  name: 'Post',
  filePathPattern: '**/*.(md|mdx)',
  contentType: 'mdx',
  fields: {
    title: {
      type: 'string',
      description: 'Title of the post',
      required: true,
    },
    tags: {
      type: 'list',
      of: { type: 'string' },
      description: 'Tags of the post',
      required: true,
    },
    description: {
      type: 'string',
      description: 'Description of the post',
      required: true,
    },
    date: {
      type: 'date',
      description: 'Date of the post',
      required: true,
    },
    serise: {
      type: 'string',
      description: 'serise of the post',
      required: false,
    },
  },
  computedFields: {
    fixedTitle: {
      type: 'string',
      resolve: (post) => titleFilter(post.title),
    },
    url: {
      type: 'string',
      resolve: (post) => `/posts/${titleFilter(post.title)}`,
    },
  },
}));

export default makeSource({
  contentDirPath: 'public/posts',
  documentTypes: [Post],
});