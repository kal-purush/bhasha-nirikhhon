import { useState, useCallback } from 'react';

// import { MarsPhotoStructure, ProtoMarsPhotoStructure} from '../models/marsPhoto' Santiago lo está haciendo

// import { } from '../models/api.model'

export type UseMarsStructure = ReturnType<typeof usePhotos>;
export function usePhotos(repo: PhotosApiRepo) {
  const initialState: MarsPhotoStructure[] = [];

  const [photos, setPhotos] = useState(initialState);

  console.log('PHOTOS: ', photos);

  const handlerError = (error: Error) => {
    console.log(error.message);
  };

  const loadPhoto = useCallback(async () => {
    try {
      const photos = await repo.loadPhotos();
      setPhotos(photos);
    } catch (error) {
      handlerError(error as Error);
    }
  }, [repo]);

  const addPhoto = async (photo: ProtoMarsPhotoStructure) => {
    try {
      const finalPhoto = await repo.createTask(photo);
      setPhotos([...photos, finalPhoto]);
    } catch (error) {
      handlerError(error as Error);
    }
  };

  const deletePhoto = async (id: MarsPhotoStructure['id']) => {
    try {
      await repo.delete(id);
      setPhotos(photos.filter((item) => item.id !== id));
    } catch (error) {
      handlerError(error as Error);
    }
  };

  const updatePhoto = async (photo: MarsPhotoStructure) => {
    try {
      const finalPhoto = await repo.update(photo);
      setPhotos(
        photos.map((item) => (item.id === photo.id ? finalPhoto : item))
      );
    } catch (error) {
      handlerError(error as Error);
    }
  };

  return {
    photos,
    loadPhoto,
    addPhoto,
    deletePhoto,
    updatePhoto,
  };
}