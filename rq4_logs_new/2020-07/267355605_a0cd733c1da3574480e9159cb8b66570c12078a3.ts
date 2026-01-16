import { ComponentFixture, async, TestBed } from '@angular/core/testing';
import { InputModule } from '@tcode/input';
import { ButtonsModule } from '@tcode/buttons';
import { LiveTrackComponent } from './liveTrack.component';

describe('LiveTrackComponent', () => {
  let component: LiveTrackComponent;
  let fixture: ComponentFixture<LiveTrackComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [LiveTrackComponent],
      imports: [
        InputModule,
        ButtonsModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LiveTrackComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
})