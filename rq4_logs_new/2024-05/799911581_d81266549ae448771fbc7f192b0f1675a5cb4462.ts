export const ROUTE_SECTIONS = {
  JOBS: 'jobs' as const,
  ORGS: 'organizations' as const,
  PROJECTS: 'projects' as const,
} as const;
export type RouteSection = (typeof ROUTE_SECTIONS)[keyof typeof ROUTE_SECTIONS];

export const HREFS = {
  HOME_PAGE: '/',
  JOBS_PAGE: `/${ROUTE_SECTIONS.JOBS}`,
  ORGS_PAGE: `/${ROUTE_SECTIONS.ORGS}`,
  PROJECTS_PAGE: `/${ROUTE_SECTIONS.PROJECTS}`,
} as const;

export const A11Y = {
  LINK: {
    BACK: 'Back',
    SIDEBAR: {
      JOBS: 'Jobs',
      ORGS: 'Organizations',
      PROJECTS: 'Projects',
    },
  },
} as const;