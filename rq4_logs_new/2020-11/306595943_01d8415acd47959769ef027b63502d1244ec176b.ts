import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NavComponent } from './nav.component';

import { MatToolbarHarness } from '@angular/material/toolbar/testing'
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatButtonHarness } from '@angular/material/button/testing'
import { MatButtonModule } from '@angular/material/button'
import { HarnessLoader } from '@angular/cdk/testing';
import { TestbedHarnessEnvironment } from '@angular/cdk/testing/testbed';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { RouterTestingModule } from '@angular/router/testing';

describe('NavComponent', () => {

  let component: NavComponent;
  let fixture: ComponentFixture<NavComponent>;

  let loader: HarnessLoader;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        RouterTestingModule,
        MatToolbarModule,
        MatButtonModule
      ],
      declarations: [ NavComponent ],
      schemas: [ CUSTOM_ELEMENTS_SCHEMA ],
    })
    .compileComponents();
    fixture = TestBed.createComponent(NavComponent);
    loader = TestbedHarnessEnvironment.loader(fixture);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(fixture.componentInstance).toBeTruthy();
  });

  it(`should render toolbar`, async () => {
    const toolbar = await loader.getHarness(MatToolbarHarness);
    expect (toolbar).toBeTruthy();
  });

  it(`should have title text inside toolbar`, async () => {
    const toolbar = await loader.getHarness(MatToolbarHarness);
    const text = (await toolbar.getRowsAsText())[0];
    expect (text).toContain(fixture.componentInstance.title);
  });

  it(`should have button`, async () => {
    const button = await loader.getHarness(MatButtonHarness);
    expect (button).toBeTruthy();
  });

});