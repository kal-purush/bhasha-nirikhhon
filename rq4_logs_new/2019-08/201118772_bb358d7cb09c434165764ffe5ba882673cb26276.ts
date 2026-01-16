import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { CalendarComponent } from './calendar/calendar.component';
import { FormPanelComponent } from './form-panel/form-panel.component';
import { CalendarService } from './calendar/calendar.service';

@NgModule({
    imports: [CommonModule, FormsModule],
    exports: [CalendarComponent, FormPanelComponent],
    declarations: [CalendarComponent, FormPanelComponent],
    providers: [CalendarService],
})
export class ComponentsModule { }