import { createElement } from './utils';
import { promoCodes } from '../components/constants';

class Slider {
  index: number;

  maxLength: number;

  constructor() {
    this.index = 0;
    this.maxLength = promoCodes.length;
    setInterval(this.nextSlide.bind(this), 5000);
  }

  private nextSlide(): void {
    if (this.index === this.maxLength - 1) {
      this.index = 0;
      this.prepareCurrentSlide(this.index);
    } else {
      this.index++;
      this.prepareCurrentSlide(this.index);
    }
  }

  private prepareCurrentSlide(index: number): void {
    const promoContainer = document.querySelector('.promo__container') as HTMLDivElement;

    const item = this.drawPromo(promoCodes[index], index);
    promoContainer.innerHTML = '';
    promoContainer?.append(item);
  }

  private drawPromo(promocode: { [key: string]: string }, idx: number): HTMLDivElement {
    const promo = createElement('div', ['promo__item', `promo__item-${idx}`]) as HTMLDivElement;
    const img = createElement('div', ['promo__img', `promo__img-${idx}`]);
    const promoInfo = createElement('div', ['promo__info-block']);
    const promoHeading = createElement('div', ['promo__info-heading'], `${promocode.discount} OFF`);
    const promoText = createElement(
      'div',
      ['promo__info-text'],
      `Use Code: <span>${promocode.promocode}</span> at your cart.${
        promocode.promocode === 'BABY' ? '<div>Only for Baby Toys: 0-12 Months</div>' : ''
      }`,
    );
    const promoBtn = createElement('div', ['promo__btn-to-catalog', 'button'], 'Shop Now');

    promoInfo.append(promoHeading, promoText, promoBtn);
    promo.append(img, promoInfo);
    return promo;
  }
}

export default Slider;