import {Component, OnInit} from '@angular/core';
import {Title} from '@angular/platform-browser';

@Component({
  selector: 'app-skewed-border',
  templateUrl: './skewed-border.component.html',
  styleUrls: ['./skewed-border.component.scss']
})
export class SkewedBorderComponent implements OnInit {



  constructor(private title: Title) {
    title.setTitle('CSS Skewed Border | Creative Box Border Hover Effects | Html CSS');
  }

  ngOnInit(): void {
  }

}