import I = require("./interfaces");
export class Gallery implements I.IGallery {
  private items: I.IPainting[];
  private image: HTMLImageElement;
  private current = 0;

  constructor(image: HTMLImageElement) {
    this.image = image;
  }

  set paintings(items: I.IPainting[]) {
    this.items = items;
  }

  render(item: number = 0) {
    if (item < 0 || item >= length) {
      return;
    }

    //this.image = new HTMLImageElement();
    this.image.innerHTML = this.items[item].name;
    //this.image.src = this.items[item].src;
  }

  renderPrev() {
    if (this.current > 0) {
      this.render(this.current--);
    }
  }

  renderNext() {
    if (this.current < this.items.length - 1) {
      this.render(this.current++);
    }
  }
}