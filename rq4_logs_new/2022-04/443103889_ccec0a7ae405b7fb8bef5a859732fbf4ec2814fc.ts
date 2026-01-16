import path from 'path';

module.exports = {
  siteMetadata: {
    siteUrl: 'https://jeonghye.blog/',
    title: 'Jeonghye Blog',
  },
  plugins: [
    'gatsby-plugin-typescript',
    'gatsby-plugin-root-import',
    'gatsby-plugin-sass',
    'gatsby-plugin-image',
    {
      resolve: `gatsby-plugin-google-fonts`,
      options: {
        fonts: [`Noto+Sans+KR:100,300,400,500,700,900`],
        display: 'swap',
      },
    },
    {
      resolve: 'gatsby-source-filesystem',
      options: {
        name: `_posts`,
        path: path.resolve(`_posts`),
      },
    },
    {
      resolve: `gatsby-plugin-mdx`,
      options: {
        gatsbyRemarkPlugins: [
          {
            resolve: `gatsby-remark-images`,
            options: {
              maxWidth: 500,
              showCaptions: ['title'],
              wrapperStyle: `
                margin-bottom: 50px;
                margin-top: 50px;
                box-shadow: #bbb 0px 0px 10px 1px;`,
            },
          },
        ],
      },
    },

    `gatsby-plugin-sharp`,
    `gatsby-transformer-sharp`,
    {
      resolve: `gatsby-plugin-disqus`,
      options: {
        shortname: `jeonghye`,
      },
    },
  ],
};