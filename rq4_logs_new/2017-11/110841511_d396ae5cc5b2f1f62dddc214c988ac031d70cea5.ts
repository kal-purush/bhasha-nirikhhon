import {BehaviorSubject} from "rxjs/BehaviorSubject";
import {MozCurrentLayoutState, MozLayoutSizeObject} from "../moz-layout.service";
import {MozLayoutAreaState, MozLayoutAreaStates, MozModuleInitialConfig} from "../moz-layout.module";
import {MozLayoutConfigFactory} from "../class/moz-layout-config-factory";

export class MozServiceDataLoad {

    private currentLayoutSize: MozLayoutSizeObject;
    private availableLayoutStates: MozLayoutAreaStates;
    private currentLayoutState: MozCurrentLayoutState;

    private config: MozModuleInitialConfig;

    constructor() {
        this.config = this.getLayoutConfigFromLocalStorage();
    }

    public getAreaStateNameByValue(areaKey: string, value: number, undefinedState: string = 'transition'): string {
        this.isValidArea(areaKey);
        const findState = this.availableLayoutStates[areaKey].find((availableStates: MozLayoutAreaState) => {
            return availableStates.value === value;
        });
        if (!findState) {
            return undefinedState;
        }
        return findState.state;
    }


    public getAreaStateByName(areaKey: string, state: string): MozLayoutAreaState {
        this.isValidArea(areaKey);
        const findState = this.availableLayoutStates[areaKey].find((availableStates: MozLayoutAreaState) => {
            return availableStates.state === state;
        });

        if (!findState) {
            throw new Error('Area state with value [' + state + '] do not exist');
        }

        return findState;
    }

    public getAreaStates(areaKey: string) {
        this.isValidArea(areaKey);
        return this.availableLayoutStates[areaKey];
    }

    public getAreaSize(areaKey: string) {
        this.isValidArea(areaKey);
        return this.currentLayoutSize[areaKey];
    }

    public getCurrentAreaStateName(areaKey: string) {
        this.isValidArea(areaKey);
        const areaSize: number = this.getAreaSize(areaKey);
        return this.getAreaStateNameByValue(areaKey,areaSize);

    }

    public isValidArea(areaKey: string) {
        if (this.availableLayoutStates.hasOwnProperty(areaKey)) {
            return true;
        } else {
            throw new Error('areaKey [' + areaKey + '] is invalid');
        }
    }

    public getLayoutConfigFromLocalStorage(): MozModuleInitialConfig {
        const config = JSON.parse(localStorage.getItem('MOZ_LAYOUT_CONFIG'));
        const configMerger = new MozLayoutConfigFactory(config);
        return configMerger.getMergedConfig();
    }

    private initLayoutBehaviorSubjects(){
        this.currentLayoutState = {
            TH: new BehaviorSubject(this.getCurrentAreaStateName('TH')),
            MH: new BehaviorSubject(this.getCurrentAreaStateName('MH')),
            BH: new BehaviorSubject(this.getCurrentAreaStateName('BH')),
            LS: new BehaviorSubject(this.getCurrentAreaStateName('LS')),
            LC: new BehaviorSubject(this.getCurrentAreaStateName('LC')),
            RC: new BehaviorSubject(this.getCurrentAreaStateName('RC')),
            RS: new BehaviorSubject(this.getCurrentAreaStateName('RS')),
            TF: new BehaviorSubject(this.getCurrentAreaStateName('TF')),
            MF: new BehaviorSubject(this.getCurrentAreaStateName('MF')),
            BF: new BehaviorSubject(this.getCurrentAreaStateName('BF'))
        }
    }
}