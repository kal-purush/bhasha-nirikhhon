interface IWork {
  title: string;
  tools: string[];
  img: string;
  link: string;
  gitLink: string;
}

export const works: IWork[] = [
  {
    title: 'Subscription manager',
    tools: ['React', 'MobX', 'react-colorful', 'use-debounce'],
    img: 'https://images.ctfassets.net/j2v0fdcyld1e/2Fx6Q02KYFrK2VKlrh5PZ7/4ef04ca6412f12fb55b372ace2cfc435/Subscription-manager.png?h=680&q=50',
    link: 'https://subscription-manaher.netlify.app/',
    gitLink: 'https://github.com/D1White/subscription-manager',
  },
  {
    title: 'Terminal website',
    tools: ['HTML5', 'CSS3', 'SCSS', 'Gulp'],
    img: 'https://images.ctfassets.net/j2v0fdcyld1e/3epdN5nqr99q5gK90PRsKD/43df5df61ae98c07c0b62dc4c0df6557/Screenshot_1.jpg?h=680&q=50',
    link: 'https://dwhite.netlify.app/',
    gitLink: 'https://github.com/D1White/terminal_website',
  },
];