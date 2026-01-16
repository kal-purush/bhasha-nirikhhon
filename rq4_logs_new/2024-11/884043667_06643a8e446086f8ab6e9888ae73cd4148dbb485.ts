import { Component, OnInit } from '@angular/core';
import { IPublications } from '../../models/i-publications';
import { PublicationService } from '../../service/publications.service';

@Component({
  selector: 'app-list-publications',
  templateUrl: './list-publications.component.html',
  styleUrl: './list-publications.component.css'
})
export class ListPublicationsComponent implements OnInit {
  publications: IPublications [] = []


  constructor(private publicationService: PublicationService){}

  ngOnInit(): void {
    this.loadPublications();
  }

  loadPublications(): void{


  }
}