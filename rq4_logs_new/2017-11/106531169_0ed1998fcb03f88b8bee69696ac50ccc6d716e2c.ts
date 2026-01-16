export enum ComponentTypes {
  INTERNAL = 'INTERNAL',
  OSS = 'OSS',
  COTS = 'COTS',
  FREESOFTWARE = 'FREESOFTWARE',
  INNER_SOURCE = 'INNER_SOURCE',
  SERVICE = 'SERVICE'
}

export const COMPONENT_TYPES = [
  ComponentTypes.INTERNAL,
  ComponentTypes.OSS,
  ComponentTypes.COTS,
  ComponentTypes.FREESOFTWARE,
  ComponentTypes.INNER_SOURCE,
  ComponentTypes.SERVICE
];

export interface ComponentDataLayout {
  name: string;
  description: string;
  createdOn: string;
  type: string; // always 'compnent
  componentType: ComponentTypes;
  createdBy: ResolvedCreatedBy; // has self
  vendors: string[]; // 
  releases: ResolvedRelease[]
}

export interface ResolvedCreatedBy {
  email: string;
  href: string;
}

export interface ResolvedRelease {
  name: string;
  version: string;
  href: string;
}

const MOCK_NAMES = ['Apache2', 'Angular', 'Linux Kernel', 'SW360'];

const MOCK_DESCRIPTIONS = [

  `Angular (commonly referred to as "Angular 2+" or "Angular 2") is a
  TypeScript-based open-source front-end web application platform led by
  the Angular Team at Google and by a community of individuals and corporations
  to address all of the parts of the developer's workflow while building
  complex web applications. Angular is a complete rewrite from the same team
  that built AngularJS.`,

  `The Apache HTTP Server, colloquially called Apache (/əˈpætʃiː/ ə-PA-chee),
  is free and open-source cross-platform web server software, released under the 
  terms of Apache License 2.0. Apache is developed and maintained by an open 
  community of developers under the auspices of the Apache Software Foundation.`,

  `Linux (/ˈlɪnəks/ (About this sound listen) LIN-əks)[9][10] is a name which broadly 
  denotes a family of free and open-source software operating systems (OS) built 
  around the Linux kernel. Typically, Linux is packaged in a form known as a 
  Linux distribution (or distro for short) for both desktop and server use. 
  The defining component of a Linux distribution is the Linux kernel,[11] an 
  operating system kernel first released on September 17, 1991, by Linus Torvalds.`,

  `A software component catalogue application - designed to work with FOSSology.
  SW360 is a liferay portal application to maintain your projects / products and the 
  software components within. It can send files to the open source license scanner 
  FOSSology for checking the license conditions and maintain license information.`

];

const MOCK_CREATED_ON = [
  '2016-12-15',
  '2017-10-11',
  '2014-12-07',
  '2016-01-13',
];

const MOCK_CREATED_BY: ResolvedCreatedBy[] = [
  { email: 'dagobert@duck.de', href: 'selflink' },
  { email: 'info@duck.de', href: 'selflink' },
  { email: 'important@duck.de', href: 'selflink' },
  { email: 'tick@duck.de', href: 'selflink' }
];

const MOCK_RELEASES: ResolvedRelease[] = [
  { name: 'Angular 5.0.0', version: '5.0.0', href: 'selflink' },
  { name: 'Apache 2.0.1', version: '2.0.1', href: 'selflink' },
  { name: 'Linux Kernel 1.0.0', version: '1.0.0', href: 'selflink' },
  { name: 'SW360 5.0.0', version: '5.0.0', href: 'selflink' },
];

const MOCK_VENDORS = ['Vanilla', 'Tedious', 'Goolf', 'LinkWins'];

export const componentDataLayoutFactory = (amount: number)  => {
  const componentDataLayouts: ComponentDataLayout[] = [];
  for (let i = 0; i < amount; i++) {
    let c: ComponentDataLayout = {
      name: MOCK_NAMES[randomArraySelector(MOCK_NAMES.length)],
      description: MOCK_DESCRIPTIONS[randomArraySelector(MOCK_DESCRIPTIONS.length)],
      createdOn: MOCK_CREATED_ON[randomArraySelector(MOCK_CREATED_ON.length)],
      type: 'component', // always 'compnent'
      componentType: COMPONENT_TYPES[randomArraySelector(COMPONENT_TYPES.length)],
      createdBy: MOCK_CREATED_BY[randomArraySelector(MOCK_CREATED_BY.length)],
      vendors: MOCK_VENDORS,
      releases: MOCK_RELEASES
    }
    componentDataLayouts.push(c);
  }
  return componentDataLayouts;
}

const randomArraySelector = (arrayLength: number) => Math.floor(Math.random() * arrayLength);