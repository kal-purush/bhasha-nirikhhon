import Button from './button';
import Container from './container';
import Img from './img';
import Overlay from './overlay';
import Page from './page';
import Qrcode from './qrcode';
import Text from './text';
export { default as AppContent } from './AppContent';
export { default as useApp } from './useApp';

export const MagicUiPage = Page;

const ui: Record<string, any> = {
  page: Page,
  container: Container,
  button: Button,
  img: Img,
  text: Text,
  qrcode: Qrcode,
  overlay: Overlay,
};

export default ui;