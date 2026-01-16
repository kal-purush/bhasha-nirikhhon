import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HeaderComponent } from './header.component';
import {By} from "@angular/platform-browser";

describe('HeaderComponent', () => {
  let component: HeaderComponent;
  let fixture: ComponentFixture<HeaderComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HeaderComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(HeaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('theme changing',()=>{
    it('should onColorThemeChange function add dark mode ',  ()=> {
      component.onColorThemeChange()
      fixture.detectChanges()

      const bodyWithDarkMode=fixture.debugElement.query(By.css('.dark-mode'))

      expect(bodyWithDarkMode).toBeTruthy()
    });

    it('should onColorThemeChange function remove dark mode ',  ()=> {
      document.body.classList.add('dark-theme')
      component.onColorThemeChange()
      fixture.detectChanges()

      const bodyWithDarkMode=fixture.debugElement.query(By.css('.dark-theme'))

      expect(bodyWithDarkMode).toBeFalsy()
    });
  })
});