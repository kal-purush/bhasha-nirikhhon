import { NO_ERRORS_SCHEMA } from "@angular/core";
import { TestBed } from "@angular/core/testing";
import { AppComponent } from "./app.component";

describe('App', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [AppComponent],
      // Shallow testing
      schemas: [NO_ERRORS_SCHEMA]
    });
  });
  it('should work', () => {
    let fixture = TestBed.createComponent(AppComponent);
    let comp = fixture.componentInstance;
    expect(comp instanceof AppComponent).toBe(true, 'should create AppComponent');
  });
});