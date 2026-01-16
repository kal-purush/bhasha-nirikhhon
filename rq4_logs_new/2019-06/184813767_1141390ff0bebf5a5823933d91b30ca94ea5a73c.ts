import coverDifferent from './assets/covers/different.jpg';
import coverLoveBizarre from './assets/covers/love_bizarre.jpg';
import coverTheRoadBeforeMe from './assets/covers/the_road_before_me.jpg';
import coverTvTatarGirl from './assets/covers/tv_tatar_girl.jpg';
import coverIdeliaByLeron from './assets/covers/idelia_by_leron.jpg';


interface IVideo {
  id: number;
  title: string;
  tooltip: string;
  url: string;
  cover: string;
  promo?: boolean;

  youtube?: string;
  iframe?: string;
}

export const videos: IVideo[] = [
  {
    id: 1,
    title: 'Different',
    tooltip: '',
    url: 'https://www.youtube.com/embed/_qQYNjPbboM',
    cover: coverDifferent,
    promo: true,

    iframe: 'https://www.youtube.com/embed/_qQYNjPbboM?controls=0&modestbranding=1&enablejsapi=1',
  },


  {
    id: 2,
    title: 'Love bizarre',
    tooltip: '',
    url: 'https://www.youtube.com/embed/heSCtd9LZss',
    cover: coverLoveBizarre,
    youtube: 'https://www.youtube.com/watch?v=heSCtd9LZss&feature=youtu.be',
    iframe: '<iframe width="560" height="315" src="https://www.youtube.com/embed/heSCtd9LZss" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>',
  },


  {
    id: 3,
    title: 'The road before me',
    tooltip: '',
    url: 'https://www.youtube.com/embed/tAz7-pgGo8c',
    cover: coverTheRoadBeforeMe,
    youtube: 'https://www.youtube.com/watch?v=tAz7-pgGo8c&feature=youtu.be',
    iframe: '<iframe width="560" height="315" src="https://www.youtube.com/embed/tAz7-pgGo8c?controls=0" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>',
  },


  {
    id: 4,
    title: 'tv Tatar girl',
    tooltip: '',
    url: 'https://www.youtube.com/embed/O_MipF8mz_w',
    cover: coverTvTatarGirl,
    youtube: 'https://www.youtube.com/watch?v=O_MipF8mz_w&feature=youtu.be',
    iframe: '<iframe width="560" height="315" src="https://www.youtube.com/embed/O_MipF8mz_w?controls=0" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>',
  },

  {
    id: 5,
    title: 'Idelia by Leron',
    tooltip: '',
    url: 'https://www.youtube.com/embed/dhtkYDaODYc',
    cover: coverIdeliaByLeron,
    youtube: 'https://www.youtube.com/watch?v=dhtkYDaODYc&feature=youtu.be',
    iframe: '<iframe width="560" height="315" src="https://www.youtube.com/embed/dhtkYDaODYc?controls=0" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>',
  },
];