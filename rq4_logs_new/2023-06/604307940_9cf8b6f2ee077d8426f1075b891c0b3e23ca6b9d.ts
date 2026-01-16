import { NgModule } from "@angular/core";
import { GenericModalComponent } from "./generic-modal.component";
import { CommonModule } from "@angular/common";

@NgModule({
  imports: [CommonModule],
  declarations: [
    GenericModalComponent
  ],
  exports: [GenericModalComponent],
})
export class GenericModalModule { }