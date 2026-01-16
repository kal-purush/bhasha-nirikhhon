import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AppComponent } from './app.component';
import { HttpserviceService } from './httpservice.service';
import { FormsModule } from '@angular/forms';
import { SecondComponent } from './second/second.component';
import { ColorDirective } from './color.directive';
import { HttpModule } from '@angular/http';

describe('AppComponent', () => {

    let component: AppComponent;
    let fixture: ComponentFixture<AppComponent>;

    beforeEach(async(() => {
        TestBed.configureTestingModule({
            declarations: [AppComponent, SecondComponent, ColorDirective],
            providers: [HttpserviceService],
            imports: [FormsModule, HttpModule],
        })
            .compileComponents();
    }));

    beforeEach(() => {
        fixture = TestBed.createComponent(AppComponent);
        component = fixture.componentInstance;

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
        expect(component.title).toBe('Hello World!');
    });
});