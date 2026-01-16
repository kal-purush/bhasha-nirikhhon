import { Component, Input, Output, EventEmitter } from '@angular/core';

import { WaitButtonBlock, WaitButton } from '@/core/types';

@Component({
  selector: 'wait-button-block',
  templateUrl: './wait-button-block.component.html',
  styleUrls: ['./wait-button-block.component.scss'],
})
export class WaitButtonBlockComponent {
  @Input() block: WaitButtonBlock;
  @Output() remove = new EventEmitter<void>();

  addButton(): void {
    const button: WaitButton = {
      show: '',
      value: '',
    };
    this.block.buttons.push(button);
  }

  deleteButton(index: number): void {
    this.block.buttons.splice(index, 1);
  }
}