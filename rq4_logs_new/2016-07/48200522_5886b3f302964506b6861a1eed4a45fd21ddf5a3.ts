namespace United {

    export module StackControls {

        export interface ActionConstructor {
            keyboard : string[] = [];
            mButton : number[] = [];
            gamepad : number[] = [];
            gButton : number[] = [];
            gAxis : number[] = [];
            gTrigger : number[] = [];
        }
        
        class AtLeastOne {
            action Action;

            constructor(action : Action) {
                this.action = action;
            }

            down() : boolean {
                this.action.keyboard.forEach(key => {
                    if (Sup.Input.isKeyDown(key)) return true;
                });
                
                this.action.gamepad.forEach(gamepad => {
                    this.action.gButton.forEach(button => {
                        if (Sup.Input.isGamepadButtonDown(gamepad, button)) return true;
                    });
                });

                this.action.mButton.forEach(button => {
                    if (Sup.Input.isMouseButtonDown(button) return true;
                });

                return false;
            }

            pressed(options?: { autoRepeat?: boolean; }) : boolean {
                this.action.keyboard.forEach(key => {
                    if (Sup.Input.wasKeyJustPressed(key, options)) return true;
                });
                
                this.action.gamepad.forEach(gamepad => {
                    this.action.gButton.forEach(button => {
                        if (Sup.Input.wasGamepadButtonJustPressed(gamepad, button)) return true;
                    });
                    this.action.gAxis.forEach(axisButton => {
                        if (Sup.Input.wasGamepadAxisJustPressed(gamepad, axisButton, true, options)) return true;
                    });
                });

                this.action.mButton.forEach(button => {
                    if (Sup.Input.wasMouseButtonJustPressed(button) return true;
                });

                return false;
            }

            released() : boolean {
                this.action.keyboard.forEach(key => {
                    if (Sup.Input.wasKeyJustReleased(key)) return true;
                });
                
                this.action.gamepad.forEach(gamepad => {
                    this.action.gButton.forEach(button => {
                        if (Sup.Input.wasGamepadButtonJustReleased(gamepad, button)) return true;
                    });
                    this.action.gAxis.forEach(axisButton => {
                        if (Sup.Input.wasGamepadAxisJustReleased(axisButton, button, true)) return true;
                    });
                });

                this.action.mButton.forEach(button => {
                    if (Sup.Input.wasMouseButtonJustReleased(button) return true;
                });

                return false;
            }
        }

        class Combined {
            action Action;

            constructor(action : Action) {
                this.action = action;
            }

            down() : boolean {
                this.action.keyboard.forEach(key => {
                    if (!Sup.Input.isKeyDown(key)) return false;
                });
                
                this.action.gamepad.forEach(gamepad => {
                    this.action.gButton.forEach(button => {
                        if (!Sup.Input.isGamepadButtonDown(gamepad, button)) return false;
                    });
                });

                this.action.mButton.forEach(button => {
                    if (!Sup.Input.isMouseButtonDown(button) return false;
                });

                return true;
            }

            pressed() : boolean {
                this.action.keyboard.forEach(key => {
                    if (!Sup.Input.wasKeyJustPressed(key, options)) return false;
                });
                
                this.action.gamepad.forEach(gamepad => {
                    this.action.gButton.forEach(button => {
                        if (!Sup.Input.wasGamepadButtonJustPressed(gamepad, button)) return false;
                    });
                    this.action.gAxis.forEach(axisButton => {
                        if (!Sup.Input.wasGamepadAxisJustPressed(gamepad, axisButton, true, options)) return false;
                    });
                });

                this.action.mButton.forEach(button => {
                    if (!Sup.Input.wasMouseButtonJustPressed(button) return false;
                });

                return true;
            }

            released() : boolean {
                this.action.keyboard.forEach(key => {
                    if (!Sup.Input.wasKeyJustReleased(key)) return false;
                });
                
                this.action.gamepad.forEach(gamepad => {
                    this.action.gButton.forEach(button => {
                        if (!Sup.Input.wasGamepadButtonJustReleased(gamepad, button)) return false;
                    });
                    this.action.gAxis.forEach(axisButton => {
                        if (!Sup.Input.wasGamepadAxisJustReleased(axisButton, button, true)) return false;
                    });
                });

                this.action.mButton.forEach(button => {
                    if (!Sup.Input.wasMouseButtonJustReleased(button) return false;
                });

                return true;
            }
        }

        export class Action {
            private keyboard : string[];
            private mButton : number[];
            private gamepad : number[];
            private gButton : number[];
            private gAxis : number[];
            private gTrigger : number[];

            public atLeastOne : AtLeastOne;
            public combined : Combined;

            constructor(touches : ActionConstructor) {
                this.keyboard = touches.keyboard;
                this.mButton = touches.mButton;
                this.gamepad = touches.gamepad;
                this.gButton = touches.gButton;
                this.gAxis = touches.gAxis;
                this.gTrigger = touches.gTrigger;

                this.atLeastOne = new AtLeastOne(this);
                this.combined = new Combined(this);
            }

            getAxisValues() : number[][] {
                if (this.gamepad) {
                    const ret : any[] = []
                    const gplength : number = this.gamepad.length;
                    for (let i : number = 0; i < gplength; i++) {
                        ret[i] = [];
                        if (this.gAxis) {
                            const galength : number = this.gAxis.length;
                            for (let j : number = 0; j < galength; j++)
                                ret[i][j] = Sup.Input.getGamepadAxisValue(this.gamepad[i], this.gAxis[j]);
                        }
                    }
                    return ret;
                }
                return undefined;
            }

            getTriggersValues() : any {
                if (this.gamepad) {
                    const ret : any[] = []
                    const gplength : number = this.gamepad.length;
                    for (let i : number = 0; i < gplength; i++) {
                        ret[i] = [];
                        if (this.gTrigger) {
                            const gtlength : number = this.gTrigger.length;
                            for (let j : number = 0; j < gtlength; j++)
                                ret[i][j] = Sup.Input.getGamepadAxisValue(this.gamepad[i], this.gTrigger[j]);
                        }
                    }
                    return ret;
                }
                return undefined;
            }

            addKey(periphType : string, newKey : string | number) {
                this[periphType].push(newKey);
            }

            editKey(periphType : string, oldKey : string | number, newKey : string | number) {
                const indexKey : number = this[periphType].indexOf(oldKey);
                this[periphType][indexKey] = newKey;
            }

            removeKey(periphType : string, oldKey : string | number) {
                let indexKey = this[periphType].indexOf(oldKey);
                delete this[periphType][indexKey];
            }
        }

        export abstract class Controls {

            constructor (ctrl : { [key:string] : Action }) {
                for (const key in ctrl) {
                    this[key.toUpperCase()] = ctrl[key];
                }
            }

            addKey(actionName : string, periphType : string, newKey : string | number) {
                this[actionName.toUpperCase()].addKey(periphType, newKey);
            }

            editKey(actionName : string, periphType : string, oldKey : string | number, newKey : string | number) {
                this[actionName.toUpperCase()].editKey(periphType, oldKey, newKey);
            }

            removeKey(actionName : string, periphType : string, oldKey : string | number) {
                this[actionName.toUpperCase()].removeKey(periphType, oldKey);
            }

            addAction(actionName : string, action : Action) {
                this[actionName.toUpperCase()] = action;
            }

            editAction(actionName : string, action : string[]) {
                this[actionName.toUpperCase()] = action;
            }

            renameAction(oldName : string, newName : string) {
                oldName = oldName.toUpperCase();
                this[newName.toUpperCase()] = this[oldName];
                delete this[oldName];
            }

            removeAction(actionName : string) {
                delete this[actionName.toUpperCase()];
            }

        }

    }
}