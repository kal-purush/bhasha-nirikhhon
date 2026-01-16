/**
 * file: Homepage Config file
 * date: 2020-07-23
 * author: Frank
 * lastModify: Frank 2020-07-23
 */

/**
 * file: this is the header components config file
 * date: 2020-07-28
 * author: Frank
 * lastModify: Frank 2020-07-28
 */
/**
 * This is the header file interface
 */
interface HeaderConfig {
    menuItem: Array<{ menu: string; link: string }>;
}
const headerConfig: HeaderConfig = {
    menuItem: [
        { menu: 'home', link: '/' },
        { menu: 'team', link: '/team' },
        { menu: 'resources', link: '/resources' },
        { menu: 'aboutUs', link: '/aboutUs' },
        { menu: 'contact', link: '/contact' },
    ],
};

export default headerConfig;