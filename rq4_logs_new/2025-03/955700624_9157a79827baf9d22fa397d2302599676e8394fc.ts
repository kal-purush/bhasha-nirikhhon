const FOOTER_LINKS = {
    information: {
        title: 'Informasi',
        links: [
            { label: 'Sejarah', href: '/history' },
            { label: 'Berita', href: '/news' },
            { label: 'Silsilah', href: '/silsilah' },
            { label: 'Galeri', href: '/galeri' },
        ],
    },
    connectWithUs: {
        title: 'Terhubung Bersama Kami',
        links: [
            {
                label: 'Facebook',
                href: 'https://www.facebook.com/dokterpost/',
                icon: '/svgs/icons/facebook.svg',
                alt: 'facebook-icon',
            },
            {
                label: 'Instagram',
                href: 'https://www.instagram.com/dokterpost/',
                icon: '/svgs/icons/instagram.svg',
                alt: 'instagram-icon',
            },
        ],
    },
    resources: {
        title: 'Resources',
        links: [{ label: 'Support', href: '/resources/help' }],
    },
    company: {
        title: 'Lainnya',
        links: [
            { label: 'Kontak', href: '/kontak' },
        ],
    },
};

export { FOOTER_LINKS };