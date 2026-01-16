import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ButtonsModule } from '@tcode/buttons';
import { BecomeAVendorComponent } from './becomeaVendor.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

describe('LandingComponent', () => {
  let component: BecomeAVendorComponent;
  let fixture: ComponentFixture<BecomeAVendorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BecomeAVendorComponent ],
      imports: [
        ButtonsModule,
        RouterTestingModule,
        RouterTestingModule,
        BrowserAnimationsModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BecomeAVendorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});