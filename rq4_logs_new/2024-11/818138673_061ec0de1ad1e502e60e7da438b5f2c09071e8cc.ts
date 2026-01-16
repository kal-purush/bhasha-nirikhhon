/**
 * 이미지를 리사이징하고 webp로 변환하는 함수
 */
interface ResizeOptions {
  width?: number; // 리사이즈할 가로 크기
  height?: number; // 리사이즈할 세로 크기
}

export const resizeAndConvertToWebp = (file: File, { width, height }: ResizeOptions): Promise<Blob> => {
  return new Promise((resolve, reject) => {
    if (file.type === 'image/gif') {
      resolve(file); // GIF는 변환하지 않고 원본 반환
      return;
    }

    const img = new Image();
    const reader = new FileReader();

    reader.onload = (e) => {
      img.src = e.target?.result as string;
    };

    reader.readAsDataURL(file);

    img.onload = () => {
      const canvas = document.createElement('canvas');
      const ctx = canvas.getContext('2d');

      if (!ctx) {
        reject(new Error('Canvas context is not available'));
        return;
      }

      const aspectRatio = img.width / img.height;

      let targetWidth: number;
      let targetHeight: number;

      if (width && height) {
        // width와 height 둘 다 주어진 경우
        targetWidth = width;
        targetHeight = height;
      } else if (width) {
        // width만 주어진 경우 원본 비율에 따라 height 계산
        const aspectRatio = img.height / img.width;
        targetWidth = width;
        targetHeight = width / aspectRatio;
      } else if (height) {
        // height만 주어진 경우 원본 비율에 따라 width 계산
        targetWidth = height * aspectRatio;
        targetHeight = height;
      } else {
        // width와 height 둘 다 없는 경우 원본 크기 사용
        targetWidth = img.width;
        targetHeight = img.height;
      }

      canvas.width = targetWidth;
      canvas.height = targetHeight;

      ctx.drawImage(img, 0, 0, targetWidth, targetHeight);

      canvas.toBlob((blob) => {
        if (blob) {
          const resizedFile = new File([blob], file.name, { type: 'image/webp' });
          resolve(resizedFile);
        } else {
          reject(new Error('Failed to create a blob from the canvas'));
        }
      }, 'image/webp');
    };

    img.onerror = () => {
      reject(new Error(`Image loading failed`));
    };
  });
};