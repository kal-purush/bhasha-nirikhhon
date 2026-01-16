import { ChangeEvent, useState } from 'react';

const useMultipleImageUpload = () => {
  const [imageSrcs, setImageSrcs] = useState<string[]>([]);
  const [imageFiles, setImageFiles] = useState<File[]>([]);

  const encodeFileToBase64 = (file: File) => {
    return new Promise<string>((resolve) => {
      const reader = new FileReader();
      reader.readAsDataURL(file);

      reader.onload = () => {
        if (typeof reader.result === 'string') {
          resolve(reader.result);
        }
      };
    });
  };

  const handleMultipleImagePreview = async (e: ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      const selectedFiles = Array.from(e.target.files);

      // 최대 이미지 개수 4개로 제한
      if (imageSrcs.length + selectedFiles.length > 4) {
        alert('이미지는 최대 4개까지 등록 가능합니다.');
        return;
      }

      const newBase64Images = await Promise.all(selectedFiles.map((file) => encodeFileToBase64(file)));

      setImageSrcs((prev) => [...prev, ...newBase64Images]);
      setImageFiles((prev) => [...prev, ...selectedFiles]);
    }
  };

  return { imageSrcs, imageFiles, handleMultipleImagePreview };
};

export default useMultipleImageUpload;