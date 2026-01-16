import {
  Component,
  HostListener,
  ElementRef,
  OnInit,
  NgZone
} from '@angular/core';

@Component({
  selector: 'app-genres-filter',
  templateUrl: './genres-filter.component.html',
  styleUrls: ['./genres-filter.component.css']
})

export class GenresFilterComponent implements OnInit {

  public genres: any[];
  public search: string = '';
  public isOpenGenresFilter: boolean = false;
  public element: HTMLElement;
  public zone: NgZone;

  public constructor(zone: NgZone,
                     element: ElementRef) {
    this.element = element.nativeElement;
    this.zone = zone;
  }

  public ngOnInit(): void {
  }

  @HostListener('document:click', ['$event'])

  public openCloseCountriesFilter(isOpenGenresFilter: boolean): void {
    this.isOpenGenresFilter = !isOpenGenresFilter;
    this.search = '';
  }
}