/// <reference path="RangeSliderParams.ts" />
/// <reference path="RangeSliderUI.ts" />

module haathi {
    export module ui {
        export module controls {
            export class RangeSliderController {

                private sliderUI: RangeSliderUI;

                constructor(private sliderParams: RangeSliderParams) {
                    this.sliderUI = new RangeSliderUI(sliderParams, this);
                }

                public slideLimitMinMax(event: Event, ui: any): boolean {
                    //var index: number = $(ui.handle).index();
                    var index: number = this.getHandleMoved(ui);
                    if (index % 2 === 0) {
                        return this.handleMin(index, ui.values[index], ui.values[index + 1],
                            this.sliderParams.getMinLimit(), this.sliderParams.getMaxLimit());
                    } else {
                        return this.handleMax(index, ui.values[index - 1], ui.values[index],
                            this.sliderParams.getMinLimit(), this.sliderParams.getMaxLimit());
                    }
                }

                private getHandleMoved(ui: any): number {
                    console.log(ui);
                    var idMoved: string = ui.handle.id;
                    console.log(idMoved);
                    return parseInt(idMoved.substring(1, idMoved.indexOf('-')), 10);
                }

                private handleMin(index: number, startVal: number, endVal: number,
                    lowerLimit: number, upperLimit: number): boolean {
                    var diff = endVal - startVal;
                    var status: boolean = false;
                    console.log("handleMin ", index, startVal, endVal, lowerLimit, upperLimit);
                    if (startVal >= lowerLimit && startVal < upperLimit) {
                        if (diff < this.sliderParams.getMinMaxGap()){
                            if (endVal < upperLimit) {
                                this.sliderUI.incrementHandleAndUpdateDisplay(index, endVal);
                                status = true;
                            }
                        } else {
                            status = true;
                        }
                    }
                    if (status) {
                        this.sliderUI.notifyHandleChange(index, startVal);
                    }
                    return status;
                }

                private handleMax(index: number, startVal: number, endVal: number,
                    lowerLimit: number, upperLimit: number): boolean {
                    var diff = endVal - startVal;
                    var status: boolean = false;
                    console.log("handleMax ", index, startVal, endVal, lowerLimit, upperLimit);
                    if (endVal > lowerLimit && endVal <= upperLimit) {
                        if (diff < this.sliderParams.getMinMaxGap()) {
                            if (startVal > lowerLimit) {
                                this.sliderUI.decrementHandleAndUpdateDisplay(index, startVal);
                                status = true;
                            }
                        } else {
                            status = true;
                        }
                    }
                    if (status) {
                        this.sliderUI.notifyHandleChange(index, endVal);
                    }
                    return status;
                }
            }
        }
    }
}

$(document).ready(() => {
    var sliderParams: haathi.ui.controls.RangeSliderParams =
        new haathi.ui.controls.RangeSliderParams("haathi-slider-range", 0, 5, 1,1);
    var app = new haathi.ui.controls.RangeSliderController(sliderParams);
});