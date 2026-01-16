/* eslint-disable max-classes-per-file */
import AppStage, { getAppStage } from '../constant/app_stage';
import { GetENVOrThrow } from '../util/setup';

interface AppConfig {
  GalleryTableName: string
  GalleryBucketName: string
}

class DefaultConfig implements AppConfig {
  GalleryTableName: string

  GalleryBucketName: string;

  constructor() {
    this.GalleryTableName = GetENVOrThrow('GALLERY_TABLE_NAME');
    this.GalleryBucketName = GetENVOrThrow('GALLERY_BUCKET_NAME');
  }
}

class TestConfig implements AppConfig {
  GalleryTableName = ''

  GalleryBucketName = ''
}

const getConfig = () : AppConfig => {
  if (getAppStage() === AppStage.Test) {
    console.log('using test config');
    return new TestConfig();
  }
  return new DefaultConfig();
};

const AppConfig = getConfig();

export default AppConfig;