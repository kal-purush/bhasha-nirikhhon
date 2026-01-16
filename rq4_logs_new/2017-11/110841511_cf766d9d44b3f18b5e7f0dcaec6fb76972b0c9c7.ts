import {MozLayoutInitialStates, MozLayoutStates} from '../defaults.constant';
import {MozLayoutAreaState, MozModuleInitialConfig} from '../moz-layout.module';

export class MozLayoutConfigFactory {

    private config: MozModuleInitialConfig = {
        initialStates: MozLayoutInitialStates,
        availableStates: MozLayoutStates
    };
    providedConfig: any;

    public static instanceOfMozLayoutAreaState(object: any): object is MozLayoutAreaState {
        const state = 'state' in object && (typeof object['state'] === 'string');
        const value = 'value' in object && (typeof object['value'] === 'number');
        return state && value;
    }

    constructor(config: any) {
        this.providedConfig = config;
    }

    getMergedConfig() {
        try {
            this.mergeAvailableStates();
            this.mergeInitialStates();
            return this.config;
        } catch (error) {
            console.log(error);
            return {
                initialStates: MozLayoutInitialStates,
                availableStates: MozLayoutStates
            };
        }
    }


    private ifIsArrayOfMozLayoutAreaState(array: any): boolean {
        try {
            if (Array.isArray(array)) {
                array.forEach((arrayItem: any) => {
                    if (!MozLayoutConfigFactory.instanceOfMozLayoutAreaState(arrayItem)) {
                        throw new Error('Provided state is not valid state');
                    }
                });
                return true;
            } else {
                throw new Error('Provided object is not array');
            }
        } catch (error) {
            return false;
        }
    }

    private mergeAvailableStates() {
        if (this.providedConfig.hasOwnProperty('availableStates')) {
            // EXPECTING KEYS LIKE TH,MH...
            for (const areaStates in this.config.availableStates) {
                if (this.providedConfig.availableStates.hasOwnProperty(areaStates)) {

                    if (this.ifIsArrayOfMozLayoutAreaState(this.providedConfig.availableStates[areaStates])) {
                        this.config.availableStates[areaStates] = this.providedConfig.availableStates[areaStates];
                    }
                }
            }
        }
    }

    private mergeInitialStates() {
        if (this.providedConfig.hasOwnProperty('initialStates')) {
            for (const initialState in this.config.initialStates) {
                if (this.providedConfig.initialStates.hasOwnProperty(initialState)) {

                    if (this.isStateExsistInAvailableStateArray(initialState, this.providedConfig.initialStates[initialState])) {
                        this.config.initialStates[initialState] = this.providedConfig.initialStates[initialState];
                    } else {
                        throw new Error('State do not exist in availableStateArray');
                    }

                }
            }
        }
    }

    private isStateExsistInAvailableStateArray(area: string, state: string) {
        if (this.config.availableStates.hasOwnProperty(area)) {
            const findState = this.config.availableStates[area].find((availableStates: MozLayoutAreaState) => {
                return availableStates.state === state;
            });
            if (!findState) {
                throw new Error('State [' + state + '] do not exist');
            }
            return true;

        } else {
            throw new Error('Provided area do not exist in availableState object');
        }
    }

}