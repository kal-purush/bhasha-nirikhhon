module.exports = {
  title: 'Arya',
  description: '专注 Node.js 技术栈分享，从前端到Node.js再到数据库',
  plugins: [
    ['@vuepress/back-to-top'], // 返回顶部
    ['@vuepress/nprogress'], // 加载进度条
    [
      '@vuepress/pwa',
      {
        serviceWorker: true,
        updatePopup: true
      }
    ],
    {
      name: 'page-plugin',
      globalUIComponents: ['fixed']
    }
  ],
  head: [
    ['link', { rel: 'icon', href: `/favicon.ico` }],
    //增加manifest.json
    ['link', { rel: 'manifest', href: '/manifest.json' }]
  ],
  themeConfig: {
    logo: '/assets/img/logo.png',
    nav: [
      { text: '主页', link: '/' },
      {
        text: '基石',
        items: [
          { text: '博客', link: '/cornerstone/博客/博客' },
          { text: 'MarkDown', link: '/cornerstone/markdown/MarkDown' },
          { text: '计算机', link: '/cornerstone/computer/' },
          { text: '算法', link: '/cornerstone/algorithms/' }
        ]
      },
      {
        text: '前端',
        items: [
          { text: 'Vue', link: '/frontend/vue/Vue3' },
          { text: 'Webpack', link: '/frontend/webpack/webpack' },
          { text: 'Typescript', link: '/frontend/typescript/TypeScript' },
          { text: 'css', link: '/frontend/css/' }
        ]
      },
      {
        text: '产品',
        items: [{ text: 'Axure', link: '/produce/axure/Axure' }]
      },
      {
        text: '后端',
        items: [{ text: 'NodeJs', link: '/backend/node/' }]
      },
      {
        text: '开发工具',
        items: [
          { text: 'Git', link: '/tools/git/01-Git使用' },
          { text: 'Github', link: '/tools/github' },
          { text: 'Chrome', link: '/tools/chrome/Chrome' },
          { text: 'VSCode', link: '/tools/vscode' }
        ]
      }, {
        text: '系统',
        link: '/system/'
      },
      { text: '面试问题', link: '/interview/' }
    ],
    // sidebar: 'auto',
    sidebarDepth: 2,
    sidebar: {
      '/frontend/typescript/': [
        // ['', 'node目录'],
        ['TypeScript', 'TypeScript入门'],
        // ['stream', 'node核心模块-stream']
      ],
      '/frontend/vue/': [
        ['Vue3', 'Vue3入门'],
      ],
      '/tools/git/': [
        ['Git', 'Git使用'],
      ],
      '/produce/axure/': [
        ['Axure', 'Axure应用，及产品经理技能'],
      ],
      '/': 'auto'
      // '/frontend/': [
      //   ['', '前端'],
      //   {
      //     title: 'css',
      //     name: 'css',
      //     collabsable: false,
      //     children: [
      //       ['css/', '目录'],
      //       ['css/1', 'css常考面试题']
      //     ]
      //   }
      // ]
    },
    base: './',
    markdown: {
      extendMarkdown: md => {
        md.use(require("markdown-it-disable-url-encode"));
      }
    }
  }
}