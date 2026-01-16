import { InverterComponent } from "./inverter.component";
import { ComponentFixture, async, TestBed } from '@angular/core/testing';
import { InputModule } from '@tcode/input';
import { ButtonsModule } from '@tcode/buttons';
import { ProductModule } from '@tcode/product';

describe('InverterComponent', () => {
  let component: InverterComponent;
  let fixture: ComponentFixture<InverterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [InverterComponent],
      imports: [
        InputModule,
        ButtonsModule,
        ProductModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InverterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
})