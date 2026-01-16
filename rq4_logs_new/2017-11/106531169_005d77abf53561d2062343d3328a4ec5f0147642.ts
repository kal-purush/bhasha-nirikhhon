import { NgModule } from '@angular/core';
import { SharedModule } from '../shared/shared.module';
import { StoreModule } from '@ngrx/store';
import { componentReducer } from './state/component.reducer';
import { ComponentListComponent } from './components/component-list/component-list.component';
import { ComponentComponent } from './components/component/component.component';

@NgModule({
  imports: [
    SharedModule,
    StoreModule.forFeature('component', componentReducer)
  ],
  declarations: [ComponentListComponent, ComponentComponent]
})
export class ComponentModule { }