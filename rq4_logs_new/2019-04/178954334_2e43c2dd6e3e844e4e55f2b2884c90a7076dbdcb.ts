import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ItemGaleriaComponent } from './item-galeria.component';

describe('ItemGaleriaComponent', () => {
  let component: ItemGaleriaComponent;
  let fixture: ComponentFixture<ItemGaleriaComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ItemGaleriaComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ItemGaleriaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});