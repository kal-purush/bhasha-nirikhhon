import {Component, AfterViewInit, Input, OnDestroy, ElementRef, OnInit, ComponentFactoryResolver} from '@angular/core';
import {BaseService} from './../../base-component/base.service';
import {BigDeltaViewService} from './big-delta.view.service';
import {Http, Headers, Response} from '@angular/http';
import {BaseComponent} from './../../base-component/base.component';
import {Observable} from 'rxjs/Rx';

declare var textFit: any;

@Component({
    selector: 'delta-view',
    moduleId: module.id,
    templateUrl: 'big-delta-view.component.html',
    providers: [BigDeltaViewService]
})
export class BigDeltaViewComponent extends BaseComponent {
    @Input() serviceURL: string;
    http: Http;
    errorMessage: any;
    message: number;
    plant: string = "";
    title: string;
    station: string;
    state: string;
    date: string;
    time: string;
    delta: string;
    typeLabel: string;
    typeNumber: string;
    imagePath: string;
    cycleColor: string = "gray";
    constructor(_http: Http, public el: ElementRef, componentResolver: ComponentFactoryResolver) {
        super(el);
        this.http = _http;
    }

    public getSmilyColor = function () {
        return "linear-gradient(to top," + this.cycleColor + ",white," + this.cycleColor + ")";
    };

    ngOnInit() {
        this.plant = "";
        this.title = "";
        this.state = "";
         this.baseService = new BigDeltaViewService(this.serviceURL, this.http);
        this.action();
    }

    private action() {
        this.baseService.dataChanged.subscribe((res: any) => {
            try {
                this.setavailableState();
                res = res.ViewData.AndonAllData.AndonAllData
                this.imagePath = "app/images/" + res.AndonModuleData.IMAGE;
                this.cycleColor = res.BigAndonData.Smiley;

                this.plant = res.AndonModuleData.PLANT;
                let nElm: any = $(this.el.nativeElement);
                let element = nElm.find(".divPlant" + this.componentID)[0];
                let widthTopDiv = element.parentElement.offsetWidth;
                element.innerHTML = this.plant;
                nElm = $(".divPlant" + this.componentID);
                nElm.boxfit({ width: widthTopDiv });

                this.title = res.AndonModuleData.TITLE;
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divTitle" + this.componentID)[0];
                element.innerHTML = this.title;
                nElm = $(".divTitle" + this.componentID);
                nElm.boxfit({ width: widthTopDiv });

                this.station = res.AndonModuleData.STATION;
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divStation" + this.componentID)[0];
                element.innerHTML = this.station;
                nElm = $(".divStation" + this.componentID);
                nElm.boxfit({ width: widthTopDiv });


                this.dofitCircle();

                nElm = $(this.el.nativeElement);
                let widthdivTimeFormat = element = nElm.find(".divTimeFormat" + this.componentID)[0].offsetWidth;
                this.state = "Aktualisiert";
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divState" + this.componentID)[0];
                element.innerHTML = this.state;
                nElm = $('.divState' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });

                var queryTime = res.BigAndonData.QueryTime;
                //date
                var d = queryTime.split("T");
                this.date = d[0];
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divDate" + this.componentID)[0];
                element.innerHTML = this.date;
                nElm = $('.divDate' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });

                var d = queryTime.split("T");
                var newd = d[1];
                var newds = newd.split(".");
                this.time = newds[0];
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divTime" + this.componentID)[0];
                element.innerHTML = this.time;
                nElm = $('.divTime' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });



                //let widthdivTypeFormat = $(this.el.nativeElement).find(".divTypeFormat"+this.componentID)[0].offsetWidth;
                this.typeLabel = "Type";
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divType" + this.componentID)[0];
                element.innerHTML = this.typeLabel;
                nElm = $('.divType');
                nElm.boxfit({ width: widthdivTimeFormat });

                this.typeNumber = res.BigAndonData.TypeNumber;
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divTypeNumber" + this.componentID)[0];
                element.innerHTML = this.typeNumber;
                nElm = $('.divTypeNumber' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });

                nElm = $(this.el.nativeElement);
                let widthcontainerDelta = nElm.find(".circleDelta" + this.componentID)[0].offsetWidth;
                this.delta = res.BigAndonData.Delta;
                nElm = $(this.el.nativeElement);
                element = nElm.find(".containerDelta" + this.componentID)[0];
                element.innerHTML = this.delta;
                element.style.height = widthcontainerDelta;
                nElm = $('.containerDelta' + this.componentID);
                nElm.boxfit({ width: widthcontainerDelta });
            } catch (error) {

                this.imagePath = "images/status_unknown.ico";
                this.cycleColor = "gray";
                this.dofitCircle();

                let nElm: any = $(this.el.nativeElement);
                let element: any;
                let widthdivTimeFormat = element = nElm.find(".divTimeFormat" + this.componentID)[0].offsetWidth;
                this.state = "unbekannt";
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divState" + this.componentID)[0];
                element.innerHTML = this.state;
                nElm = $('.divState' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });

                var currentdate = new Date();
                //date               
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divDate" + this.componentID)[0];
                element.innerHTML = currentdate.toLocaleDateString();
                nElm = $('.divDate' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });

                nElm = $(this.el.nativeElement);
                element = nElm.find(".divTime" + this.componentID)[0];
                element.innerHTML = currentdate.toLocaleTimeString();
                nElm = $('.divTime' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });
                this.typeLabel = "Type";
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divType" + this.componentID)[0];
                element.innerHTML = this.typeLabel;
                nElm = $('.divType');
                nElm.boxfit({ width: widthdivTimeFormat });

                this.typeNumber = "?";
                nElm = $(this.el.nativeElement);
                element = nElm.find(".divTypeNumber" + this.componentID)[0];
                element.innerHTML = this.typeNumber;
                nElm = $('.divTypeNumber' + this.componentID);
                nElm.boxfit({ width: widthdivTimeFormat });

                nElm = $(this.el.nativeElement);
                let widthcontainerDelta = nElm.find(".circleDelta" + this.componentID)[0].offsetWidth;
                this.delta = "?";
                nElm = $(this.el.nativeElement);
                element = nElm.find(".containerDelta" + this.componentID)[0];
                element.innerHTML = this.delta;
                element.style.height = widthcontainerDelta;
                nElm = $('.containerDelta' + this.componentID);
                nElm.boxfit({ width: widthcontainerDelta });
            }
        },
            error => this.errorMessage = <any>error);
    }

    ngOnDestroy() {
        super.ngOnDestroy();
    }

    dofitCircle() {
        let nElm: any = $(this.el.nativeElement);
        let divCircle = nElm.find(".divCircle" + this.componentID)[0];
        nElm = $(this.el.nativeElement);
        let middleTop = nElm.find(".middleTop" + this.componentID)[0];
        nElm = $(this.el.nativeElement);
        let circlesDelta = nElm.find(".circleDelta" + this.componentID)[0];
        //let widthdivDelta = $(this.el.nativeElement).find('.divDelta'+this.componentID)[0].offsetWidth;
        if (middleTop.offsetHeight > divCircle.offsetWidth) {
            circlesDelta.style.width = divCircle.offsetWidth * 0.9 + "px";
            circlesDelta.style["padding-bottom"] = divCircle.offsetWidth * 0.9 + "px";
        }
        else {
            circlesDelta.style.width = middleTop.offsetHeight * 0.9 + "px";
            circlesDelta.style["padding-bottom"] = middleTop.offsetHeight * 0.9 + "px";
        }
    }
}