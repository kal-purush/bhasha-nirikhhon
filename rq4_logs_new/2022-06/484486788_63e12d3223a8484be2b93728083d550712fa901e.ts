// choose background image by supply a color name

interface BackgroundPictureMap {
  [key: string]: {
    src: string;
    alt: string;
  };
}

const backgroundPicture: BackgroundPictureMap = {
  home: {
    src: "/images/henry-be--Pg63JThyCg-unsplash.jpg",
    alt: "ladder by book shelves in an classical library; by Henry De on Unsplash",
  },
  texturedDarkGrayWall: {
    src: "/images/annie-spratt-6a3nqQ1YwBw-unsplash.jpg",
    alt: "textured darkgray wall; by Annie Spratt on Unsplash",
  },
};

export { backgroundPicture };