import { ToasterConfig } from 'angular2-toaster/angular2-toaster';

export const myToasterConfig:ToasterConfig =
  new ToasterConfig({
    showCloseButton: false,
    tapToDismiss: true,
    timeout: 2000,
    positionClass: 'toast-bottom-full-width'
  });