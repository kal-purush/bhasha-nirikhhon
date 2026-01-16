import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ZigsawpuzzleComponent } from './zigsawpuzzle.component';

describe('ZigsawpuzzleComponent', () => {
  let component: ZigsawpuzzleComponent;
  let fixture: ComponentFixture<ZigsawpuzzleComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ZigsawpuzzleComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ZigsawpuzzleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});