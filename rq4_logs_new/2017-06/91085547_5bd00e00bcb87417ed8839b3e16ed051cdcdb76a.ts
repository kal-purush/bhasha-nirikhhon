
import BABYLON from "../../static/babylon";
import {RealmClass} from "../Realm/Realm";
import {IObject} from "../ObjectFactory/ObjectFactory";
import {OfflineGameState} from "../States/OfflineGameState";

declare const Realm: RealmClass;

export class Explosion extends BABYLON.Mesh implements IObject {

    public sphere : any;

    constructor(name: string, scene: BABYLON.Scene) {
        super(name, scene);

        this.sphere = BABYLON.Mesh.CreateSphere(
            'explosion',
            100,
            30,
            scene,
        );

        this.sphere.material = new BABYLON.StandardMaterial('expl', scene);
        this.sphere.material.emissiveColor = new BABYLON.Color3(1, 1, 1);

        this.sphere.scaling = new BABYLON.Vector3(1, 1, 1);
    }

    onCreate(): void {
        // throw new Error("Method not implemented.");
    }

    onGrab(): void {
        this.setEnabled(true);
        (<OfflineGameState> Realm.state).explosions.push(this);
    }

    onFree(): void {
        this.setEnabled(false);

        if (!Realm.state || !(<OfflineGameState> Realm.state).explosions) {
            return;
        }

        (<OfflineGameState> Realm.state).explosions.splice(
            (<OfflineGameState> Realm.state).explosions.indexOf(this), 1);
    }

    onRender(): void {
        (<OfflineGameState> Realm.state).ships.forEach(ship => {
            if (Math.random() > 0.7) {
                (<OfflineGameState> Realm.state).explodeAt(ship.position);
            }
        })
    }

    onDelete(): void {
        this.dispose(true);
    }

}