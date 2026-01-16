import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import Keyboard from "simple-keyboard";

@Component({
  selector: 'app-simple-keyboard',
  templateUrl: './simple-keyboard.component.html',
  styleUrls: ['../../../../node_modules/simple-keyboard/build/css/index.css', './simple-keyboard.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class SimpleKeyboardComponent implements OnInit {
  value = '';
  keyboard: Keyboard;

  constructor(
  ) { }

  public ngOnInit(): void {
  }

  ngAfterViewInit() {
    this.keyboard = new Keyboard({
      onChange: input => this.onChange(input),
      onKeyPress: button => this.onKeyPress(button)
    });
  }

  onChange = (input: string) => {
    this.value = input;
    console.log("Input changed", input);
  };

  onKeyPress = (button: string) => {
    console.log("Button pressed", button);

    /**
     * If you want to handle the shift and caps lock buttons
     */
    if (button === "{shift}" || button === "{lock}") this.handleShift();
  };

  onInputChange = (event: any) => {
    this.keyboard.setInput(event.target.value);
  };

  handleShift = () => {
    let currentLayout = this.keyboard.options.layoutName;
    let shiftToggle = currentLayout === "default" ? "shift" : "default";

    this.keyboard.setOptions({
      layoutName: shiftToggle
    });
  };

}