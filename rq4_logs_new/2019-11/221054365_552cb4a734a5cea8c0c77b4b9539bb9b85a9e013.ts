import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { AppComponent } from './app.component';
import { MenuHeaderComponent } from './menu-header/menu-header.component';
import { MenuBlockComponent } from './menu-block/menu-block.component';
import { MenuBlockTitleComponent } from './menu-block-title/menu-block-title.component';
import { MenuService } from './menu.service';

@NgModule({
  imports:      [ BrowserModule, FormsModule ],
  declarations: [ AppComponent, MenuHeaderComponent, MenuBlockComponent, MenuBlockTitleComponent ],
  bootstrap:    [ AppComponent ],
  providers: [MenuService]
})
export class AppModule { }