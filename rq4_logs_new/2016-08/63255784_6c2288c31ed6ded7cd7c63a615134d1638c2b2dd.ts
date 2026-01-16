import {
  TestBed
} from '@angular/core/testing';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { CMModule } from './index';
import { CmCollectionComponent } from './cm-collection.component';
import { CmCollectionItemDirective } from './cm-collection-item.directive';




describe('Component: CmCollection', () => {

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [ CMModule, FormsModule ],
      declarations: [ TestComponent ],
    });
  });

  it('should create the cmColleciton component and one collection item directive', ( done ) => {

    let fixture = TestBed.createComponent(TestComponent);
    fixture.detectChanges();

    fixture.whenStable().then( () => {

      let debElem = fixture.debugElement.query(By.directive(CmCollectionComponent));
      expect(debElem).toBeDefined('CmCollectionComponent not found');

      let cmCollectionComponent = debElem.componentInstance;
      expect(cmCollectionComponent.cmItems.size).toBe(1, 'missing the collection item diretive instance');

      let debElems = fixture.debugElement.queryAll(By.directive(CmCollectionItemDirective));
      expect(debElems.length).toBe(1, `there is no or there are too much collection item`);

      // the component instance is null - why?
      // console.log(debElems[0]);
      // let cmCollectionItem = debElems[0].componentInstance;
      // console.log(cmCollectionItem);

      done();

    });
  });

  it('should remove the colleciton item from the collection if the component is destroyed', () => {
    let fixture = TestBed.createComponent(TestComponent);
    fixture.detectChanges();

    fixture.whenStable().then( () => {

      let collectionInstance = fixture.debugElement.query(By.directive(CmCollectionComponent)).componentInstance;

      expect(collectionInstance.cmItems.size).toBe(1, 'there shoudl be one item');

      fixture.componentInstance.items = [];
      fixture.detectChanges();

      expect(collectionInstance.cmItems.size).toBe(0, 'the item should have been removed');

    });

  });


});


@Component({
  selector: 'test-component',
  template: `
    <cm-collection [(ngModel)]="selectedItems">
      <div *ngFor="let item of items" >
        <input [(ngModel)]="item.selected" [cmItem]="item">
      </div>
    </cm-collection>
  `
})
class TestComponent {

  public items = [
    { selected: false }
  ];

  public selectedItems = [];

}