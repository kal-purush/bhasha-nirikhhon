import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { <%=classify(name)%>Component } from './<%=dasherize(name)%>/<%=dasherize(name)%>.component';
import { <%=classify(name)%>Service } from './<%=dasherize(name)%>.service';



@NgModule ({
  imports: [
    CommonModule,
  ],
  declarations: [
    <%=classify(name)%>Component
  ],
  entryComponents: [],
  providers: [<%=classify(name)%>Service ],
  exports: [
    <%=classify(name)%>Component
  ],
})
export class <%=classify(name)%>Module {
}