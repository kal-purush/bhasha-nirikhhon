const canvas = document.createElement('canvas');
const context = canvas.getContext('2d');

type RGBTuple = [number, number, number];

// ImageData -> List (RGBTuple)
const getRGBtuples = (imageData: ImageData): Array<RGBTuple> => {
  const rgbArr = [];

  let i, rgbVal;
  for (i = 0; i < imageData.data.length; i += 4) {
    rgbVal = [
      imageData.data[i],
      imageData.data[i + 1],
      imageData.data[i + 2]
    ];
    rgbArr.push(rgbVal);
  }

  return rgbArr;
};

// RGBTuple -> Bool
const isWhite = (rgbVal: RGBTuple): boolean => {
  return rgbVal.every(color => color === 255);
};

// Int -> Float -> Image -> Bool
const hasWhiteBackground = (borderSize: number, threshold: number, image: HTMLImageElement): boolean => {
  canvas.width = image.width;
  canvas.height = image.height;

  context.drawImage(image, 0, 0);

  // context.getImageData(xStart, yStart, rectWidth, rectHeight);
  const topBorder = context.getImageData(0, 0, image.width, borderSize);
  const bottomBorder = context.getImageData(0, image.height - borderSize, image.width, borderSize);
  const leftBorder = context.getImageData(0, 0, borderSize, image.height);
  const rightBorder = context.getImageData(image.width - borderSize, 0, borderSize, image.height);

  const borderArray = [topBorder, leftBorder, bottomBorder, rightBorder];

  const rgbArrays = borderArray
    .map(getRGBtuples);

  const numberOfWhites = rgbArrays
    .map(rgbArray => {
      return rgbArray
        .reduce((acc, rgbString) => {
          if (isWhite(rgbString)) {
            return ++acc;
          };

          return acc;
        }, 0);
    })
    .reduce((acc, curr) => acc + curr, 0);

  const totalPixels = rgbArrays.reduce((acc, curr) => acc + curr.length, 0);

  return numberOfWhites > (totalPixels * threshold);
};

export default hasWhiteBackground;