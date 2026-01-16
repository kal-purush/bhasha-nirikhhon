import { Component, OnInit } from '@angular/core';
import historicalTextsData from '../../../assets/historicalTexts.json'
import { HistoricalTexts } from '../interfaces/historicalTexts.interface';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit {
  historicalTexts:  HistoricalTexts[] = historicalTextsData.historicalTexts
  
  ngOnInit(): void {
  
  }
}