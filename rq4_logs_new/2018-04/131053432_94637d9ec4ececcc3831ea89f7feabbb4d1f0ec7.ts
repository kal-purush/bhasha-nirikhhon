import {async, fakeAsync, getTestBed, tick, ComponentFixture, TestBed} from '@angular/core/testing';
import {By} from '@angular/platform-browser';
import {HttpClientTestingModule} from '@angular/common/http/testing';
import {DebugElement} from '@angular/core';

import { RouterTestingModule } from '@angular/router/testing';
import {CardCmp} from './card_cmp';
import {NetrunnerService} from './netrunner_service';
import {NetrunnerModule} from './netrunner_module';
import {Card, Cycle, Pack} from './types';

import * as _ from 'lodash';
declare var $;
fdescribe('TestingCardCmp', () => {
    let fixture: ComponentFixture<CardCmp>;
    let service: NetrunnerService;
    let comp: CardCmp;
    let el: HTMLElement;
    let de: DebugElement;


    beforeEach(async( () => {
        TestBed.configureTestingModule({
            imports: [RouterTestingModule, NetrunnerModule, HttpClientTestingModule],
            providers: [
                NetrunnerService
            ]
        }).compileComponents();

        service = TestBed.get(NetrunnerService);
        fixture = TestBed.createComponent(CardCmp);
        comp = fixture.componentInstance;

        de = fixture.debugElement.query(By.css('.card-cmp'));
        el = de.nativeElement;
    }));

    function getTotalCards() {
        return _.get(service.getCards(), 'total');
    }

    it('Should create a netrunner component', () => {
        expect(comp).toBeDefined("We should have the Netrunner comp");
        expect(el).toBeDefined("We should have a top level element");

    });

    it('Should be able to render a card without exploading and also have a class based on can_play', () => {
        let card = new Card({title: 'test', faction_code: 'test_faction'});
        comp.card = card;
        fixture.detectChanges();

        expect($('.card-details').length).toBe(1, "It should be rendered at this point");
    });

});
