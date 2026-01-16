///<reference path="../src/color.ts"/>
///<reference path="../src/theme.ts"/>

module NineTails.DevTools {
  export class ColorEditor {

    private _color: NineTails.Color;
    public element: HTMLElement;

      constructor(color: NineTails.Color, theme: NineTails.Theme) {
        this._color = color;
        this.element = document.createElement('div');

        var colorBlock = document.createElement('div');
        colorBlock.style.width = '20px';
        colorBlock.style.height = '20px';
        colorBlock.style.margin = '10px';
        colorBlock.style.border = '1px solid #eee';
        colorBlock.className = 'color-' + theme.colors.indexOf(color);
        this.element.appendChild(colorBlock);

        var rule = theme.createRule('.color-' + theme.colors.indexOf(color));
        rule.linkStyle('background-color', color);

        var redSlider = document.createElement('input');
        redSlider.type = 'range';
        redSlider.min = '0';
        redSlider.max = '255';
        redSlider.value = color.red.toString();
        redSlider.style.width = '80px';
        redSlider.onchange = function () {
          color.set(parseInt(redSlider.value), color.green, color.blue);
        }

        this.element.appendChild(redSlider);

        var greenSlider = document.createElement('input');
        greenSlider.type = 'range';
        greenSlider.min = '0';
        greenSlider.max = '255';
        greenSlider.value = color.green.toString();
        greenSlider.style.width = '80px';
        greenSlider.onchange = function () {
          color.set(color.red, parseInt(greenSlider.value), color.blue);
        }

        this.element.appendChild(greenSlider);

        var blueSlider = document.createElement('input');
        blueSlider.type = 'range';
        blueSlider.min = '0';
        blueSlider.max = '255';
        blueSlider.value = color.blue.toString();
        blueSlider.style.width = '80px';
        blueSlider.onchange = function () {
          color.set(color.red, color.green, parseInt(blueSlider.value));
        }

        this.element.appendChild(blueSlider);
    }
  }
}