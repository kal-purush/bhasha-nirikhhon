import AppleIcon from '../icons/AppleIcon';
import AndroidIcon from '../icons/AndroidIcon';
import WebIcon from '../icons/WebIcon';
import MultiPlatformIcon from '../icons/MultiPlatformIcon';

export interface TFormCardConfig {
  formIndex: number;
  title: string;
  description: string;
  // backButton?: React.ReactNode;
  // fwdButton?: React.ReactNode;
  options: {
    optionIndex: number;
    title: string;
    icon: React.ReactNode;
    optimisticHours?: number;
    pessmisticHours?: number;
  }[];
}

export const FormConfigs: TFormCardConfig[] = [
  {
    formIndex: 1,
    title: 'What type of platform are you building?',
    description:
      'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
    options: [
      {
        optionIndex: 1,
        title: 'Apple iOS',
        icon: AppleIcon,
        optimisticHours: 40,
        pessmisticHours: 57,
      },
      {
        optionIndex: 2,
        title: 'Android',
        icon: AndroidIcon,
        optimisticHours: 40,
        pessmisticHours: 57,
      },
      {
        optionIndex: 3,
        title: 'Web',
        icon: WebIcon,
        optimisticHours: 40,
        pessmisticHours: 57,
      },
      {
        optionIndex: 4,
        title: 'Multi-Platform',
        icon: MultiPlatformIcon,
        optimisticHours: 80,
        pessmisticHours: 114,
      },
    ],
  },
  {
    formIndex: 2,
    title: 'How big is your platform?',
    description:
      'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
    options: [
      {
        optionIndex: 1,
        title: 'Small',
        icon: 'smallIcon',
        optimisticHours: 20,
        pessmisticHours: 29,
      },
      {
        optionIndex: 2,
        title: 'Medium',
        icon: 'mediumIcon',
        optimisticHours: 40,
        pessmisticHours: 57,
      },
      {
        optionIndex: 3,
        title: 'Large',
        icon: 'largeIcon',
        optimisticHours: 60,
        pessmisticHours: 86,
      },
    ],
  },
];

// {
//   formIndex: 2,
//   title: 'How big is your platform?',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen2Btns,
// },
// {
//   formIndex: 3,
//   title: 'Do you have a designer',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen3Btns,
// },
// {
//   formIndex: 4,
//   title: 'What level of UX/UI do you want?',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen4Btns,
// },

// {
//   formIndex: 5,
//   title: 'Do People have to login',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen5Btns,
// },
// {
//   formIndex: 6,
//   title: 'Do people create personal profiles?',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen6Btns,
// },
// {
//   formIndex: 7,
//   title: 'Does your app and website need to be connected?',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen7Btns,
// },
// {
//   formIndex: 8,
//   title: 'Are there payments involved?',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen8Btns,
// },
// {
//   formIndex: 9,
//   title: 'Do you want an admin portal?',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen9Btns,
// },
// {
//   formIndex: 10,
//   title: 'Dates and Location',
//   description:
//     'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam',
//   buttons: screen10Btns,
// },