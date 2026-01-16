import { TestBed, ComponentFixture, async } from '@angular/core/testing';
import { MatCardModule, MatChipsModule } from '@angular/material';

import { AboutComponent } from './about.component';

describe('AboutComponent', () => {
  let aboutComponent: AboutComponent;
  let aboutFixture: ComponentFixture<AboutComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        MatCardModule,
        MatChipsModule
      ],
      declarations: [
        AboutComponent
      ],
    });
  }));

  beforeEach(() => {
    aboutFixture = TestBed.createComponent(AboutComponent);
    aboutComponent = aboutFixture.componentInstance;
  });

  it('should create the about component', () => {
    expect(aboutFixture.debugElement.componentInstance).toBeTruthy();
  });
});