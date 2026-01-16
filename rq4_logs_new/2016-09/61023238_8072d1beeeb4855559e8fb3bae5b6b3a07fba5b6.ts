import { ApplicationRef, Component } from '@angular/core';
import { FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';

import { By } from '@angular/platform-browser';
import { QuickSearchComponent } from "./quickSearchBase";
import { MakerApi, CarApi } from '../../shared/services';
import { SharedModule } from '../../shared';
import reducers from '../../core/reducers';
import { StoreModule } from '@ngrx/store';

import { APP_CORE_API_PROVIDERS } from '../../core'
//import { MockRouter, MockAppController, MockApiService } from '../helpers/mocks';
import { Router, RouterModule } from '@angular/router';
import { Subject, Observable } from 'rxjs';
import { TestBed, async, inject } from '@angular/core/testing'
import { Store } from '@ngrx/store';
import { CatalogActions } from "../../core/actions";
import { AppState, getMakers, getCatalogReady } from "../../core/reducers";

@Component({
    selector: 'test-cmp',
    template: ''
})
class TestComponent { }
class MockRouter {
    navigate(value) { }
}
describe('COMPONENTS TESTS', () => {
    describe("Quich search component tests", () => {
        beforeEach(() => TestBed.configureTestingModule({
            imports: [
                RouterModule,
                FormsModule,
                ReactiveFormsModule,
                StoreModule.provideStore(reducers),
                // import Shared module
                SharedModule
            ],
            providers: [
                { provide: Router, useClass: MockRouter },
                ...APP_CORE_API_PROVIDERS
            ],
            declarations: [
                TestComponent,
                QuickSearchComponent
            ]
        }));

        beforeEach(() => {
            TestBed.overrideComponent(TestComponent, {
                set: {
                    template: '<quickSearch></quickSearch>'
                }
            });
        });

        beforeEach(inject([Router, MakerApi, CarApi, Store], (router, makerApi, carApi) => {
            spyOn(router, "navigate");
            spyOn(carApi, "count").and.returnValue(Observable.of({ count: 42 }));
            spyOn(makerApi, 'getCarModels').and.returnValue(Observable.of([]));
        }));

        it('should init', async(() => {
            TestBed.compileComponents().then(() => {
                var fixture = TestBed.createComponent(TestComponent);
                fixture.detectChanges();
                var compiled = fixture.debugElement.nativeElement;
                expect(compiled.innerHTML).toContain('Made by');
            });
        }));
        it('should contain count', async(
            inject(
                [
                    Store,
                    CatalogActions
                ], (store: Store<AppState>,
                    catalogActions: CatalogActions) => {

                    TestBed.compileComponents().then(() => {
                        var fixture = TestBed.createComponent(QuickSearchComponent);

                        var beingTestedCompInst: QuickSearchComponent = fixture
                            .debugElement
                            .componentInstance;

                        beingTestedCompInst
                            .count$
                            .subscribe((value) => {
                                expect(value).toBe(42);
                                fixture
                                    .whenStable()
                                    .then(() => {
                                        var buttonDe = fixture.debugElement.query(By.css('.btn'));
                                        expect(buttonDe.nativeElement.innerHTML).toContain('Show 412');
                                    })
                            });




                        //   beingTestedCompInst.ready$                              
                        //        .do(() => { console.log('ready') })
                        //        .subscribe(() => { })

                        store.dispatch(catalogActions.setAppLookups({
                            makers: [],
                            engineTypes: []
                        }));

                    });

                })
        ))

        // it('should seed default fields', async(inject([AppController], (appController) => {
        //     var compiled = componentFxt.debugElement.nativeElement;
        //     appController.init$.subscribe(value => {
        //         expect(value.makers).toBeDefined();
        //     });
        //     componentFxt.debugElement.componentInstance.count$.subscribe((value) => {
        //         expect(value).toBe(42);
        //         componentFxt.whenStable().then(() => {
        //             expect(componentFxt.componentInstance.carMakers[0].name).toBe('gravitsapa_motors');
        //             expect(compiled.querySelector('#maker')
        //                 .options.item(1).innerHTML).toBe('gravitsapa_motors');
        //             expect(compiled.querySelector('button').innerText).toBe('Show 42');
        //         })//.detectChanges();

        //     })
        //     appController.start();
        // })));

        // xit('should require cars models when maker change', async(inject([AppController, Api], (appController, apiBackend) => {
        //     var compiled = componentFxt.debugElement.nativeElement;

        //     appController.init$.subscribe(value => {
        //         //   let select = componentFxt.debugElement.query(By.css("select#maker"));
        //         //   dispatchEvent(select.nativeElement, "change");
        //         componentFxt.debugElement.componentInstance.carFormModel.maker = value.makers[0];
        //         componentFxt.detectChanges();
        //         componentFxt.whenStable().then(() => {
        //             expect(apiBackend.getMakerModels).toHaveBeenCalledWith(1)
        //         })
        //     });
        //     appController.start();
        // })));
        // xit('should submit and build search request params', async(inject([Router, Api], (router, apiBackend) => {
        //     componentFxt.debugElement.componentInstance.submit();
        //     expect(router.navigate).toHaveBeenCalledWith(['SearchList', {}]);
        // })));
    });
});