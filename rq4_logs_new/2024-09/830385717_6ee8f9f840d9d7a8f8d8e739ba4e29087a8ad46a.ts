import { IBrand } from '@spwntch/react-ui';

export const BRAND: IBrand = {
  logoUrl: {
    light: '/brand/logo-light.webp',
    dark: '/brand/logo-dark.webp',
  },
  markUrl: {
    light: '/brand/mark-light.webp',
    dark: '/brand/mark-dark.webp',
  },
  underConstructionUrl: {
    light: '/under-construction/powered-by-spawntech-light.webp',
    dark: '/under-construction/powered-by-spawntech-dark.webp',
  },
  tagline: 'Empowering Innovation in Experience Design and Development',
  about: {
    body: [
      'Spawntech is a cutting-edge technology company committed to delivering innovative solutions for experience design, software development, and open-source communities. Our flagship brand, The Experience Business (TXB), drives advancements in customer and user experience technology, helping businesses craft impactful interactions.',
      'Beyond experience design, Spawntech offers an open-source monorepo framework built on Nx, aimed at accelerating and simplifying full-stack development. Our `@spwntch/workspace` monorepo provides developers with a powerful toolkit, integrating the best of Nx and PNPM to streamline project creation, optimize continuous integration, and support scalable app development.',
      'At the core of Spawntech’s mission is the belief that technology should empower teams to build fast, efficient, and maintainable applications without compromising on user experience. Our toolkits, from the `spawn-workspace` CLI to our component libraries, are designed to help developers move quickly and confidently, leveraging years of UX testing and best practices.',
      'Spawntech embraces a lean mindset, enabling organizations to iterate rapidly while ensuring robust and accessible web apps. Whether through TXB’s experience design solutions or our open-source developer tools, we strive to make modern development more efficient and accessible to all.',
      'Join us in transforming how businesses and developers approach experience design and software development, with tools that help you move faster, build better, and scale smarter.'
    ],
  },
  contact: {
    address:
      '3 Sandown Valley Crescent, Sandton, Johannesburg, 2196, South Africa',
    website: 'www.spawntech.io',
    mail: 'hello@spawntech.io',
    phone: '+27 11 994 9960',
  },
  social: {
    linkedin: 'https://www.linkedin.com/company/Spawntech-io',
    // github: 'https://github.com/spwntch',
  },
  legal: {
    companyName: 'Spawntech',
    copyrightYear: '2024',
  },
};