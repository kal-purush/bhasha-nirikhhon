import { AppComponent } from "./app.component";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { DebugElement } from "@angular/core/src/debug/debug_node";
import { By } from "@angular/platform-browser";

describe('AppComponent created correctly', () => {

  let component: AppComponent;
  let componentFixture: ComponentFixture<AppComponent>;
  let debugElement: DebugElement;
  let htmlElement: HTMLElement;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [AppComponent]
    })

    componentFixture = TestBed.createComponent(AppComponent);
    component = componentFixture.componentInstance;
  })

  it('Given a main component', () => {
    it('When creating the component', () => {
      it('Then, the TODO title have to be in the h1 element'), () => {
        debugElement = componentFixture.debugElement.query(By.css('h1'));
        htmlElement = debugElement.nativeElement;
        expect(htmlElement.textContent).toContain('TODO App');
      }
    })
  })
})