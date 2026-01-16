import { Component, Input, Output, EventEmitter } from '@angular/core';

import { GameBoard } from "./../../model/gameboard";
import { GameNode } from "./../../model/gamenode";

@Component({
    selector: 'gameboard',
    template: 
    `
    <div class="gameboard" [ngStyle]="board.boardStyle">
        <ng-container *ngFor="let row of board.level">
            <ng-container *ngFor="let node of row">
                <gamenode [node]="node" (onClicked)="handleNodeClicked($event)" (onHover)="handleNodeHover($event)"></gamenode>
            </ng-container>
        </ng-container>
    </div>
    `
})

export class GameBoardComponent {
    @Input() public board: GameBoard;

    //Gamenode
    @Output() public onNodeClicked: EventEmitter<GameNode> = new EventEmitter<GameNode>();
    @Output() public onNodeHover: EventEmitter<GameNode> = new EventEmitter<GameNode>();
    handleNodeClicked(node: GameNode) { this.onNodeClicked.emit(node) }
    handleNodeHover(node: GameNode) { this.onNodeHover.emit(node) }
}