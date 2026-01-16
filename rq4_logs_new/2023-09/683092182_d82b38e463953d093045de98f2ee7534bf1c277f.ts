export type SiteConfig = typeof siteConfig;

export const siteConfig = {
    name: 'QCard',
    description: 'A site for assisting tech interview',
    url: 'https://qcard.com',
    mainNav: [
        {
            title: 'QCard',
            items: [
                {
                    title: 'Questions',
                    href: '/questions',
                    description: 'All question categories we offer',
                    items: [],
                },
                {
                    title: 'Interview',
                    href: '/interview',
                    description: 'Mock interview',
                    items: [],
                },
            ],
        },
    ],
};