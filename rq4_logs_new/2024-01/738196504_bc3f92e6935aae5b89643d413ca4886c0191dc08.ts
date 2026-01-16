import { CapacitorConfig } from '@capacitor/cli';

const config: CapacitorConfig = {
  appId: 'com.shopfolder.app',
  appName: 'shop-folder-contact',
  webDir: 'dist/shop-folder-contact',
  server: {
    androidScheme: 'https'
  }
};

export default config;