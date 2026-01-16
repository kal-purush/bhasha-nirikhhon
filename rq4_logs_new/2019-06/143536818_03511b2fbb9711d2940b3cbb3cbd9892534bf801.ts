import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserProfileSearchResultComponent } from './user-profile-search-result.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { LayoutModule } from '@angular/cdk/layout';
import { FlexLayoutModule } from '@angular/flex-layout';
import { ReactiveFormsModule } from '@angular/forms';
import { AppMaterialModule } from '../../app-material.module';
import { UserProfileSearchResult } from '../user-profile-search-result';
import { By } from '@angular/platform-browser';

const users: UserProfileSearchResult[] = [
  {
    id: 'e6b808eb-b6bd-447d-8dce-3e0d66b17759',
    userName: 'tester',
    firstName: 'Toni',
    lastName: 'Tester',
    email: 'toni.tester@skoop.io',
    manager: {
      id: 'e6b808eb-b6bd-447d-8dce-3e0d66b17759',
      userName: 'manager',
      firstName: 'Manager',
      lastName: 'Manager',
      email: 'manager.manager@skoop.io'
    }
  }
];

describe('UserProfileSearchResultComponent', () => {
  let component: UserProfileSearchResultComponent;
  let fixture: ComponentFixture<UserProfileSearchResultComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        LayoutModule,
        FlexLayoutModule,
        ReactiveFormsModule,
        AppMaterialModule
      ],
      declarations: [ UserProfileSearchResultComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserProfileSearchResultComponent);
    component = fixture.componentInstance;
    component.foundUsers = users;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fill results form', () => {
    const searchResults = fixture.debugElement.queryAll(By.css('.user-profile-search-result-card'));
    expect(searchResults.length).toBe(1);
  });
});