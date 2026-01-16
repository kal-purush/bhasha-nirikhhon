import { ApnsCertificate } from '../../build/src/index';

declare global {
    // tslint:disable-next-line:interface-name
    interface Window { fileChangeEvent: Function; }
}

function stripBase64Mime (base64: string) {
  return base64.substr(base64.indexOf(',') + 1, base64.length);
}

window.fileChangeEvent = (files: File[]) => {
  const reader = new FileReader();
  reader.onload = () => {
    try {
      const settings = new ApnsCertificate(
        stripBase64Mime(<string> reader.result),
        prompt('Does the certificate require a password?') || undefined
      );

      Object.keys(settings)
        .filter((key: string) => document.getElementById(key))
        .forEach((key: string) => {
          const elm = <HTMLInputElement> document.getElementById(key);
          elm.value = JSON.stringify(settings[key]);
      });

    } catch (err) {
      alert(err.message);
    }
  };

  reader.readAsDataURL(files[0]);
};