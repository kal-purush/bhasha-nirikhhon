export type PageList = Record<string, Page>;
export type Page = {
  name: string;
  short_name: string;
  description: '';
  og: {
    locale: string; //
    title: string;
    image: string;
    description: string;
    url: string;
  };
};

export const PAGE = (() => {
  const list: PageList = {};
  const page: Page = {
    name: 'Milo',
    short_name: 'Milo',
    description: '',
    og: {
      locale: 'en-NL',
      title: 'Milo',
      image: '/assets/banner/1200w/page.png',
      description: '',
      url: '/',
    },
  };

  list['_app'] = { ...page };
  list['_404'] = { ...page, name: `${page.name} - 404` };

  return list;
})();