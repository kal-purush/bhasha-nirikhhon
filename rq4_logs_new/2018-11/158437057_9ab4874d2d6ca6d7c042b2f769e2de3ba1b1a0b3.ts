import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TileComponent } from './tile.component';

describe('TileComponent', () => {
  let component: TileComponent;
  let fixture: ComponentFixture<TileComponent>;
  let compiledHtml: HTMLElement;
  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ TileComponent ]
    })
    fixture = TestBed.createComponent(TileComponent);
    component = fixture.componentInstance;
    compiledHtml = fixture.debugElement.nativeElement;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('renders tile text in a p tag', () => {
    const text = "This is a test tile";
    component.tileText = text;
    fixture.detectChanges();
    expect(compiledHtml.querySelector('p').textContent).toContain(text);
  })

  it("should not show text if face down", () => {
    component.tilefaceDown = true;
    fixture.detectChanges();
    expect(compiledHtml.querySelector('p')).toBeFalsy();
  })

  it('should give tile div class face-down if face down', () => {
    component.tilefaceDown = true;
    fixture.detectChanges();
    expect(compiledHtml.querySelector('div.tile').classList).toContain("face-down");
  })

  it('should give tile div class face-up if not face down', () => {
    component.tilefaceDown = false;
    fixture.detectChanges();
    expect(compiledHtml.querySelector('div.tile').classList).toContain("face-up");
  })

  it('should flip face when clicked', () => {
    component.tilefaceDown = true;
    fixture.detectChanges();
    compiledHtml.querySelector('div.tile').dispatchEvent(new Event("click"));
    expect(component.tilefaceDown).toBeFalsy();
  })
});