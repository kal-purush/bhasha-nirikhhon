import { Component } from '@angular/core';
import { Scene } from './scene';

@Component({
    selector: 'dropdown',
    templateUrl: 'app/dropdown.component.html'
})

export class Dropdown{
    scenes: Scene[];
    sceneToDisplay: Scene;
    displaySceneNumber: Number;

    constructor () {
        this.scenes = [];
        let act1scene1 = new Scene(1,1, "THIS IS ACT 1 SCENE 1");
        this.scenes.push(act1scene1);
        this.sceneToDisplay = act1scene1;
        let act1scene2 = new Scene(1,2, "THIS IS ACT 1 SCENE 2");
        this.scenes.push(act1scene2);
        let act2scene1 = new Scene(2,1, "THIS IS ACT 2 SCENE 1");
        this.scenes.push(act2scene1);
        let act2scene2 = new Scene(2,2, "THIS IS ACT 2 SCENE 2");
        this.scenes.push(act2scene2);
    }

    update(scene: Scene): void {
        console.log('Hello????');
        this.displaySceneNumber = scene.sceneNumber;
    }
}