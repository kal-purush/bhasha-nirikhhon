import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BebbRouteComponent } from './bebb-route.component';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { KuiCoreConfig, ReadResource, ReadResourcesSequence, SearchService } from '@knora/core';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';
import { BeolService } from '../services/beol.service';

describe('BebbRouteComponent', () => {
    let component: BebbRouteComponent;
    let fixture: ComponentFixture<BebbRouteComponent>;
    const lt = '1706-03-17_Hermann_Jacob-Scheuchzer_Johannes';

    let beolService: BeolService;
    let searchService: SearchService;

    beforeEach(async(() => {

        const beolServiceSpy = jasmine.createSpyObj('BeolService', ['searchForLetterFromBEBB', 'routeByResourceType']); // see https://angular.io/guide/testing#angular-testbed
        const searchServiceSpy = jasmine.createSpyObj('SearchService', ['doExtendedSearchReadResourceSequence']);

        TestBed.configureTestingModule({
            imports: [
                HttpClientModule,
                HttpClientTestingModule,
                RouterTestingModule
            ],
            declarations: [BebbRouteComponent],
            providers: [
                {
                    provide: ActivatedRoute,
                    useValue: {
                        paramMap: of({
                            get: () => {
                                return lt;
                            }
                        })
                    }
                },
                { provide: 'config', useValue: KuiCoreConfig },
                { provide: BeolService, useValue: beolServiceSpy },
                { provide: SearchService, useValue: searchServiceSpy }
            ]
        })
            .compileComponents();

        beolServiceSpy.searchForLetterFromBEBB.and.returnValue('gravsearchQuery');

        beolService = TestBed.get(BeolService);

        const mockRes = of(
            new ReadResourcesSequence(
                [new ReadResource('letterIri', 'http://0.0.0.0:3333/ontology/0801/beol/v2#letter', 'label', [], [], [], [], {})],
                1
            )
        );

        searchServiceSpy.doExtendedSearchReadResourceSequence.and.returnValue(mockRes);

        searchService = TestBed.get(SearchService);

    }));

    beforeEach(() => {
        fixture = TestBed.createComponent(BebbRouteComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should perform a query to get the letter\'s actual Iri', () => {

        expect(beolService.searchForLetterFromBEBB).toHaveBeenCalledWith(lt);

        expect(searchService.doExtendedSearchReadResourceSequence).toHaveBeenCalledWith('gravsearchQuery');

        expect(beolService.routeByResourceType).toHaveBeenCalledWith('http://0.0.0.0:3333/ontology/0801/beol/v2#letter', 'letterIri');
    });
});