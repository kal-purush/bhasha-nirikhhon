import localFont from 'next/font/local'

export const flexo = localFont({
    src: [
      {
        path: '../../public/fonts/Flexo/Flexo-Regular.woff',
        weight: '400',
        style: 'normal',
      },
      {
        path: '../../public/fonts/Flexo/Flexo-It.woff',
        weight: '400',
        style: 'italic',
      },
      {
        path: '../../public/fonts/Flexo/Flexo-Medium.woff',
        weight: '700',
        style: 'normal',
      },
      {
        path: '../../public/fonts/Flexo/Flexo-MediumIt.woff',
        weight: '700',
        style: 'italic',
      },
    ],
  })