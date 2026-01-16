import { Component, OnInit, ChangeDetectionStrategy } from '@angular/core';

@Component({
  selector: 'app-canvas-controls',
  templateUrl: './canvas-controls.html',
  styleUrls: ['./canvas-controls.css'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CanvasControlsComponent implements OnInit {

  height: number = 1;
  width: number = 1;

  constructor() {
    // Empty for now, probably going to use later
  }

  ngOnInit() {
    // Empty for now, probably going to use later
  }
  
  /**
   * Blurs the targeted input to trigger the change event
   * 
   * @param input Input on the page to target 
   */
  blurInput(input: any) {
    input.target.blur();
  }

  /**
   * Tell the canvas to create a new blank slate
   */
  createNew() {
    console.log('Create New!!!!!!!');
  }

  /**
   * Redo the last action undone
   */
  redo() {
    console.log('Redo');
  }

  /**
   * Save the current state of the canvas
   */
  save() {
    console.log('Save');
  }

  /**
   * Undo the last action done
   */
  undo() {
    console.log('Undo');
  }

  /**
   * Method to call to indicate that the height has changed
   *
   * @param height Height (in pixels)
   */
  updateHeight(height: number) {
    console.log('Height updated: ', height);
  }

  /**
   * Method to call to indicate the width has changed
   * 
   * @param width Width (in pixels)
   */
  updateWidth(width: number) {
    console.log('Width updated: ', width);
  }

}