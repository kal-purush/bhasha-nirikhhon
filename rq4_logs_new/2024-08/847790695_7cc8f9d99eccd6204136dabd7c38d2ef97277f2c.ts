import { Component } from '@angular/core';
import {MainComponent} from "./main/main.component";
import {TopBarComponent} from "./top-bar/top-bar.component";
import {RouterOutlet} from "@angular/router";

@Component({
  selector: 'app-layout',
  standalone: true,
  imports: [MainComponent, TopBarComponent, RouterOutlet],
  templateUrl: './layout.component.html',
  styleUrl: './layout.component.css'
})
export class LayoutComponent {

}