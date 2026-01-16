import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { AppMaterialModule } from '../../app-material/app-material.module';
import { UserCardComponent } from './user-card.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

describe('UserCardComponent', () => {
  let component: UserCardComponent;
  let fixture: ComponentFixture<UserCardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ AppMaterialModule, BrowserAnimationsModule ],
      declarations: [ UserCardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserCardComponent);
    component = fixture.componentInstance;
    component.user = {
      name: 'user1',
      email: 'user1@email.com',
      address: {
        street: 'user1 street',
        suite: '1',
        city: 'user1 city',
        zipcode: '123',
        geo: {
          lat: 12,
          lng: 32,
        },
      },
      website: 'www.user1.com',
    };
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});