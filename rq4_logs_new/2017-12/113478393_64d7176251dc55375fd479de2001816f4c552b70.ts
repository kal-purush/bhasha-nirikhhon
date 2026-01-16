import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { MyFirstComponentComponent } from './my-first-component.component';

describe('MyFirstComponentComponent', () => {
  let component: MyFirstComponentComponent;
  let fixture: ComponentFixture<MyFirstComponentComponent>;
  let de: DebugElement;
  let el: HTMLElement;

  // This is the first, aynchronous beforeEach.
  // Read more at: https://angular.io/guide/testing#the-first-asynchronous-beforeeach
  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MyFirstComponentComponent ]
    })
    .compileComponents();
  }));

  // This is the second, synchronous, beforeEach.
  // Read more at: https://angular.io/guide/testing#the-second-synchronous-beforeeach
  beforeEach(() => {
    fixture = TestBed.createComponent(MyFirstComponentComponent);
    component = fixture.componentInstance;

    // Query for the title <h1> by a CSS element selector.
    de = fixture.debugElement.query(By.css('h1'));
    el = de.nativeElement;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should display the original title', () => {
    fixture.detectChanges();
    expect(el.textContent).toContain(component.title);
  });

  it('should display a different test title', () => {
    component.title = 'Test Title';
    fixture.detectChanges();
    expect(el.textContent).toContain('Test Title');
  });

  it('doesn`t have a title in the DOM until we manually call `detectChanges`', () => {
    expect(el.textContent).toEqual('');
  });

});