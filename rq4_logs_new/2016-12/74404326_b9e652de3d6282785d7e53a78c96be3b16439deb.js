
import Component from './component';

export default class HomeViewHud extends Component {
  constructor ({ arrowHandler, styleHandler }) {
    super();

    let el = this.el = this.div('home-view-hud');

    let label = this.label = this.div('home-view-hud-label');
    label.addEventListener('click', styleHandler, false);
    el.appendChild(label);

    let arrowContainer = this.arrowContainer = this.div('home-view-collection-arrow-container');
    el.appendChild(arrowContainer);

    let leftArrow = this.leftArrow = this.div('home-view-hud-arrow', null, 'Previous Collection');
    leftArrow.addEventListener('click', () => {
      if (arrowHandler) arrowHandler(-1);
    }, false);
    arrowContainer.appendChild(leftArrow);

    let rightArrow = this.rightArrow = this.div('home-view-hud-arrow', null, 'Next Collection');
    rightArrow.addEventListener('click', () => {
      if (arrowHandler) arrowHandler(1);
    }, false);
    arrowContainer.appendChild(rightArrow);

    this.state = {
      titleCount: 0
    };

    this.setStyle();
  }

  setStyle (style = 'collection') {
    let names = { collection: 'Collection', crazy: 'Explosion', neat: 'Splayed' };
    this.label.textContent = `${names[style]} View`;

    this.label.className = `home-view-hud-label home-view-label-${style}`;

    if (style === 'collection') {
      this.arrowContainer.classList.add('active');
    } else {
      this.arrowContainer.classList.remove('active');
    }
  }
}