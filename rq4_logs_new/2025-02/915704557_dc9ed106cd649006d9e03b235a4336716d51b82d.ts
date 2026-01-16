import { images } from "./imageDataSet";

export const sliderConfigs = [
  {
    images: images.slice(0, 3),
    offset: 0,
    settings: {
      speed: 500,
      slidesToShow: 3,
      infinite: true,
      slidesToScroll: 1,
    },
  },
  {
    images: images.slice(3, 7),
    offset: 3,
    settings: {
      speed: 500,
      slidesToShow: 4,
      infinite: true,
      slidesToScroll: 1,
    },
  },
  {
    images: images.slice(7, 11),
    offset: 7,
    settings: {
      speed: 500,
      slidesToShow: 4,
      infinite: true,
      slidesToScroll: 1,
    },
  },
];