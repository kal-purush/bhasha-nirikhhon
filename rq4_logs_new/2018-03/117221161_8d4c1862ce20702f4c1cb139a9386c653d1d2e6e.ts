import {async, ComponentFixture, TestBed} from '@angular/core/testing';
import {HttpClientTestingModule} from '@angular/common/http/testing';
import {DebugElement} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {of} from 'rxjs/observable/of';
import {DmListViewRouteComponent} from './dm-listview-route.component';

describe('DmListViewRoute', () => {

  let component: DmListViewRouteComponent;
  let fixture: ComponentFixture<DmListViewRouteComponent>;
  let element: DebugElement;

  const page = 0;
  const sortby = '';
  const order = 'asc';
  const size = 5;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      declarations: [ DmListViewRouteComponent ],
      providers: [{
        provide: ActivatedRoute,
        useValue: {
          queryParams: of({
            page : page,
            sortby : sortby,
            order : order,
            size : size
          })
        }
      }]
    })
      .compileComponents();
  }));

  beforeEach(async(() => {
    fixture = TestBed.createComponent(DmListViewRouteComponent);
    component = fixture.componentInstance;
    element = fixture.debugElement;
    fixture.detectChanges();
  }));

  describe('when the compnent is created', () => {

    // it('has page', () => {
    //   expect(component.page).toEqual(page);
    // });
    //
    // it('has sortby', () => {
    //   expect(component.sortby).toEqual(sortby);
    // });
    //
    // it('has order', () => {
    //   expect(component.order).toEqual(order);
    // });
    //
    // it('has size', () => {
    //   expect(component.size).toEqual(size);
    // });

  });
});
