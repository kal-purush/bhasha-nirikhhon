import { Component, OnInit, HostListener, Input } from '@angular/core';
import { AirportsService } from '../airports.service';
import { Area } from '../area';
import { Airport } from '../airport';

@Component({
  selector: 'app-airport',
  templateUrl: './airport.component.html',
  styleUrls: ['./airport.component.scss']
})
export class AirportComponent implements OnInit {
  value: string = '';
  selectedAirport: any;
  airportsList: (Airport | Area)[];
  @Input() placeholder: string;
  selected = null;
  isOpened = true;



  constructor(private airportsService: AirportsService) {
  }

  // @HostListener('window:keydown', ['$event']) onArrowClicked(event: any) {
  //   if (event.keyCode === 40) {
  //     this.addToInput(this.selectedAirport)
  //     // console.log('arrow up selected air', this.selectedAirport)
  //   } else if (
  //     event.keyCode === 38) {
  //     this.addToInput(this.selectedAirport)
  //     // console.log('arrow down selected air', this.selectedAirport)
  //   }
  // }

  ngOnInit() {
    console.log(this.placeholder)
    this.airportsList = this.airportsService.getAirports();
    this.airportsList.sort((a, b) => {
      
      if ((a as Airport).airport && (b as Area).area) {
        return -1;
      }
      else if ((a as Area).area && (b as Airport).airport) {
        return 1;
      }
      else  {
        return 0;
      }
    })
  }

  addToInput(selectedAirport) {
    this.value = selectedAirport.airport;
    this.selectedAirport = selectedAirport;
    console.log(this.value, 'ori')
    console.log(this.selectedAirport.airport, 'airpo')
  
  }


  onMouseOver(airport){
    airport.selected = true;
    console.log('hello', airport.selected)
  }

  onMouseLeave(airport){
    airport.selected = false;
    console.log('hello', airport.selected)
    this.selected = null;
  }

  previousAirport() {
    if (!this.selected && this.airportsList.length) {
      this.selected = this.airportsList[0]
      this.selected.selected =true
      console.log(this.selected)
    } else {
      let newIndex = this.airportsList.findIndex(airport => airport == this.selected) - 1
      this.selected = this.airportsList[newIndex]
      this.selected.selected = true
      console.log(this.selected)
    }
      
    
  }


  nextAirport(event) {
    if (!this.selected && this.airportsList.length) {
      this.selected = this.airportsList[0]
      this.selected.selected =true
      console.log(event)
    } else {
      let newIndex = this.airportsList.findIndex(airport => airport == this.selected) + 1
      this.selected = this.airportsList[newIndex]
      this.selected.selected =true
      console.log(event)
    }
  }

  clearInput() {
    this.value = '';
  }
}