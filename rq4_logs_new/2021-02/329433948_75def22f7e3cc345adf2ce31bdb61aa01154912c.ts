import { FlightModel, FlightMultihopModel } from './../../shared/models/types';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from './../../shared/shared.module';
import { FormBuilder } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import { FlightSearchComponent } from './../components/flight-search/flight-search.component';
import { FlightsService } from './../services/flights.service';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { TripType } from '../models/types';
import { Overlay } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { FlightResultsPageComponent } from '../pages/flight-results-page/flight-results-page.component';
import { Flight } from '../models/flight';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

describe('Flight service integration tests', () => {
  let component: FlightSearchComponent;
  let fixture: ComponentFixture<FlightSearchComponent>;

  let resultsComponent: FlightResultsPageComponent;
  let resultsFixture: ComponentFixture<FlightResultsPageComponent>;

  let flightService: FlightsService;
  let httpMock: HttpTestingController;

  let departDate: Date;
  let returnDate: Date;

  let mockFlight: Flight;
  let mockMultihopFlight: Flight;
  let mockFlightModel: FlightModel;
  let mockFlightMultihopModel: FlightMultihopModel;

  let requestUrl: string;
  let requestUrlReturn: string;
  let requestUrlMultihop: string;
  let requestUrlReturnMultihop: string;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FlightSearchComponent, FlightResultsPageComponent],
      imports: [SharedModule, NoopAnimationsModule, CommonModule, HttpClientTestingModule],
      providers: [Router, MatDialog, Overlay, FormBuilder]
    })
    .compileComponents();
  });

  const initializeFlightSearch = () => {
    fixture = TestBed.createComponent(FlightSearchComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  const initializeResultsPage = () => {
    resultsFixture = TestBed.createComponent(FlightResultsPageComponent);
    resultsComponent = resultsFixture.componentInstance;
    resultsFixture.detectChanges();
  };

  const initializeSearchForm = () => {
    departDate = new Date();
    departDate.setDate(departDate.getDate() + 1);
    returnDate = new Date();
    returnDate.setDate(returnDate.getDate() + 8);

    const formData = {
      originAirport: { iataIdent: 'LAX', city: '', name: '' },
      destinationAirport: { iataIdent: 'JFK', city: '', name: '' },
      departDate,
      returnDate,
    };
    component.form.setValue(formData);
  };

  const initializeMockFlights = () => {
    const formData = component.form.value;
    mockFlight = new Flight();
    mockMultihopFlight = new Flight();
    mockFlightModel = {
      departTime: formData.departDate,
      arrivalTime: formData.returnDate,
      flightId: 1,
      seatsAvailable: 102,
      price: 250,
      flightNumber: 'UA-2509',
      flightDetails: {
        flightNumber: 'UA-2509',
        arriveCityId: formData.destinationAirport.iataIdent,
        departCityId: formData.originAirport.iataIdent
      }
    };
    mockFlightMultihopModel = { leg1: mockFlightModel, leg2: mockFlightModel };
    mockFlight.addLeg(mockFlightModel);
    mockMultihopFlight.addLeg(mockFlightModel);
    mockMultihopFlight.addLeg(mockFlightModel);
  };

  const initializeRequestUrls = () => {
    requestUrl = `${flightService.URL}?origin=${mockFlightModel.flightDetails.departCityId}`
      + `&dest=${mockFlightModel.flightDetails.arriveCityId}&date=${departDate.toLocaleDateString('en-CA')}`;
    requestUrlReturn = `${flightService.URL}?origin=${mockFlightModel.flightDetails.arriveCityId}`
      + `&dest=${mockFlightModel.flightDetails.departCityId}&date=${returnDate.toLocaleDateString('en-CA')}`;
    requestUrlMultihop = `${flightService.URL}/multihop?origin=${mockFlightModel.flightDetails.departCityId}`
      + `&dest=${mockFlightModel.flightDetails.arriveCityId}&date=${departDate.toLocaleDateString('en-CA')}`;
    requestUrlReturnMultihop = `${flightService.URL}/multihop?origin=${mockFlightModel.flightDetails.arriveCityId}`
      + `&dest=${mockFlightModel.flightDetails.departCityId}&date=${returnDate.toLocaleDateString('en-CA')}`;
  };

  beforeEach(() => {
    initializeFlightSearch();
    flightService = TestBed.inject(FlightsService);
    httpMock = TestBed.inject(HttpTestingController);
    initializeSearchForm();
    component.selectedPassengers = 1;
    component.selectedTripType = TripType.ROUND_TRIP;
    initializeMockFlights();
    initializeRequestUrls();
  });

  it('flight search component is created', () => {
    expect(component).toBeTruthy();
  });

  it('one way flight search sets flights-service properties', () => {
    component.selectedTripType = TripType.ONE_WAY;
    spyOn(flightService, 'setFlightRequest').and.callThrough();
    component.onFlightSearch();
    expect(flightService.isRoundTrip).toBeFalsy();
    expect(flightService.flightRequest).toBeTruthy();
  });

  it('round trip flight search sets flights-service properties', () => {
    component.selectedTripType = TripType.ROUND_TRIP;
    spyOn(flightService, 'setFlightRequest').and.callThrough();
    component.onFlightSearch();
    expect(flightService.isRoundTrip).toBeTruthy();
    expect(flightService.flightRequest).toBeTruthy();
  });

  it('flight results loads the correct flights for one way', () => {
    component.selectedTripType = TripType.ONE_WAY;
    component.onFlightSearch();
    spyOn(flightService, 'getDepFlights').and.callThrough();
    initializeResultsPage();
    httpMock.expectOne(requestUrl).flush([mockFlightModel]);
    httpMock.expectOne(requestUrlMultihop).flush([mockFlightMultihopModel]);
    expect(resultsComponent).toBeTruthy();
    expect(resultsComponent.departureFlights).toBeTruthy();
    expect(flightService.getDepFlights).toHaveBeenCalled();
    expect(resultsComponent.departureFlights.length).toBe(2);
    expect(resultsComponent.departureFlights).toContain(mockFlight);
    expect(resultsComponent.departureFlights).toContain(mockMultihopFlight);
  });

  it('flight results loads the correct flights for round trip', () => {
    component.selectedTripType = TripType.ROUND_TRIP;
    component.onFlightSearch();
    spyOn(flightService, 'getDepFlights').and.callThrough();
    spyOn(flightService, 'getReturnFlts').and.callThrough();
    initializeResultsPage();
    httpMock.expectOne(requestUrl).flush([mockFlightModel]);
    httpMock.expectOne(requestUrlMultihop).flush([mockFlightMultihopModel]);
    resultsComponent.departReturnToggle();
    httpMock.expectOne(requestUrlReturn).flush([mockFlightModel]);
    httpMock.expectOne(requestUrlReturnMultihop).flush([mockFlightMultihopModel]);
    expect(flightService.getDepFlights).toHaveBeenCalled();
    expect(flightService.getReturnFlts).toHaveBeenCalled();
    expect(resultsComponent.departureFlights.length).toBe(2);
    expect(resultsComponent.returnFlights.length).toBe(2);
    expect(resultsComponent.departureFlights).toContain(mockFlight);
    expect(resultsComponent.departureFlights).toContain(mockMultihopFlight);
    expect(resultsComponent.returnFlights).toContain(mockFlight);
    expect(resultsComponent.returnFlights).toContain(mockMultihopFlight);
  });
});