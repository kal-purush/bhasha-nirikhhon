import { Crop } from 'react-image-crop';

export default function cropImage(image: HTMLImageElement, crop: Crop): Promise<Blob> {
   const canvas = document.createElement('canvas');

   const scaleX = image.naturalWidth / image.width;
   const scaleY = image.naturalHeight / image.height;

   const percentWidth = (image.width * (crop.width as number)) / 100;
   const percentHeight = (image.height * (crop.height as number)) / 100;

   const percentX = (image.width * (crop.x as number)) / 100;
   const percentY = (image.height * (crop.y as number)) / 100;

   canvas.width = percentWidth;
   canvas.height = percentHeight;

   const ctx = canvas.getContext('2d') as CanvasRenderingContext2D;

   ctx.drawImage(
      image,
      percentX * scaleX,
      percentY * scaleY,
      percentWidth * scaleX,
      percentHeight * scaleY,
      0,
      0,
      percentWidth,
      percentHeight,
   );

   return new Promise((resolve, reject) => {
      canvas.toBlob((blob) => {
         resolve(blob as Blob);
      }, 'image/jpeg', 1);
   });
}