import { IContent, IMdxDoc } from '@spwntch/react-ui';

const THE_REAL_REASON_YOU_NEED_AN_ICP: IMdxDoc = {
  meta: {
    slug: 'the-real-reason-you-need-an-icp',
    date: '2024-03-11',
    coverImage: '/images/articles/the-real-reason-you-need-an-icp.webp',
    authorName: 'Dave Benjamin',
    // authorAvatar: '/images/articles/sanlam-icon.webp',
    title: 'The Real Reason You Need an ICP',
    abstract: [
      'In today\'s fast-paced market, understanding your Ideal Customer Profile (ICP) is crucial for driving business growth and gaining a competitive edge. This blog explores the significance of an ICP, how it aligns business strategies across marketing, sales, and product development, and why it’s essential for businesses aiming to enhance customer experience and loyalty. Learn how crafting a precise ICP can transform your approach to targeting and engaging with high-value customers, leading to sustainable success.'
    ],
    keywords: [
      'Ideal Customer Profile',
      'ICP',
      'customer segmentation',
      'target market',
      'business strategy',
      'customer experience',
      'marketing strategy',
      'sales strategy',
      'product development',
      'customer loyalty',
      'market research',
      'customer insights',
      'B2B marketing',
      'B2C marketing',
      'business growth',
      'competitive advantage'
    ],
  },
};

export const BLOG_POSTS: IMdxDoc[] = [THE_REAL_REASON_YOU_NEED_AN_ICP];

export const FEATURED_POSTS: IContent = {
  heading: {
    content: `Insights to Elevate Your Business`,
    className: 'max-w-3xl mx-auto',
  },
  subheading: {
    content: `Explore our expert articles on user experience, business strategy, technology, AI and innovation.`,
    className: 'max-w-3xl mx-auto',
  },
  body: {
    content: [
      `At The Experience Business, we're committed to helping you stay ahead in a rapidly changing market. Our featured posts provide deep insights and practical advice on crafting ideal customer profiles, optimizing customer journeys, and driving sustainable business growth.`,
      `Dive into our articles to learn how to leverage cutting-edge strategies and tools to enhance your competitive edge and achieve lasting success.`,
    ],
    className: 'max-w-4xl mx-auto',
  },
};