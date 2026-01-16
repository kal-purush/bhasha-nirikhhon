export enum OnBoardingRoutes {
  INTRO = '/intro',
  PARENT_DETAILS = '/step-one',
  CHILD_DETAILS = '/step-two',
  CHILD_AVATAR = '/step-three',
  REVIEW = '/review',
}

export const BASE_URL = '/courses/enroll/';

export const STEPS = [
  {
    title: 'Intro',
    route: 'intro',
    link: OnBoardingRoutes.INTRO,
  },
  {
    title: 'Parent',
    route: 'step-one',
    link: OnBoardingRoutes.PARENT_DETAILS,
  },
  {
    title: 'Child Info',
    route: 'step-two',
    link: OnBoardingRoutes.CHILD_DETAILS,
  },
  {
    title: 'Child Avatar',
    route: 'step-three',
    link: OnBoardingRoutes.CHILD_AVATAR,
  },
  { title: 'Review', route: 'review', link: '/review' },
];