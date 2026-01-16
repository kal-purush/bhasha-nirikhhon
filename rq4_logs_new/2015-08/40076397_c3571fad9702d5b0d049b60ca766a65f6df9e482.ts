///<reference path="../src/size.ts"/>
///<reference path="../src/theme.ts"/>

module NineTails.DevTools {
  export class SizeEditor {

    private _size: NineTails.Size;
    public element: HTMLElement;

      constructor(size: NineTails.Size, theme: NineTails.Theme) {
        this._size = size;
        this.element = document.createElement('div');

        var sizeBlock = document.createElement('div');
        sizeBlock.style.width = '20px';
        sizeBlock.style.height = '20px';
        sizeBlock.style.margin = '10px';
        sizeBlock.innerHTML = size.get();
        this.element.appendChild(sizeBlock);

        var valueSlider = document.createElement('input');
        valueSlider.type = 'range';
        valueSlider.min = '0';
        valueSlider.max = '20';
        valueSlider.value = size.value.toString();
        valueSlider.style.width = '80px';
        valueSlider.onchange = function () {
          size.set(parseInt(valueSlider.value));
          sizeBlock.innerHTML = size.get();
        }

        this.element.appendChild(valueSlider);
    }
  }
}