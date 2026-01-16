import Control from '../../../controls/control';
import Footer from '../../mainPage/footer';
import audioChallengeStartPage from './audioChallengeStartPage';

export default class AudioCallGameSection extends Control {
  constructor(parentNode: HTMLElement, group = Math.round(Math.random()*5), page = Math.round(Math.random()*29)) {
    super(parentNode, 'section', 'audio-challenge-wrapper', '');
    document.body.lastChild.remove();
    new audioChallengeStartPage(this.node, group, page);
  }

  destroy() {
    super.destroy();
    new Footer(document.body);
  }
}