import {Component, Input} from "@angular/core";
import {DoseSeries} from "../dose-series/dose-series";
import {ItemSliding} from "ionic-angular/index";
import {DoseSeriesService} from "../dose-series-service/dose-series.service";
import {Events} from "ionic-angular/index";

/**
 * DoseEventSlider for rendering a dose-events.
 */
@Component(
    {
        selector: "dose-series-slider",
        templateUrl: "build/dose-series-slider/dose-series-slider.component.html"
    }
)

export class DoseSeriesSliderComponent {

    @Input()
    public doseSeries: DoseSeries;

    constructor(
        private doseSeriesService: DoseSeriesService,
        private events: Events
    ) {
    }

    slidingDelete(slidingItem: ItemSliding) {
        this.doseSeriesService.delete(this.doseSeries.id).then(
            (doseSeries: DoseSeries) => {
                this.events.publish(
                    "doseSeries:deleted",
                    doseSeries as DoseSeries
                );
            }
        );
    }
}
