import { NgModule } from '@angular/core';
import {CommonModule} from "@angular/common";

import {PoemBodyAnnotation} from "./poem-body-annotation.ts";

@NgModule({
  declarations:[PoemBodyAnnotation],
  imports:[CommonModule],
  exports:[PoemBodyAnnotation]
})

export class MainPipe{}