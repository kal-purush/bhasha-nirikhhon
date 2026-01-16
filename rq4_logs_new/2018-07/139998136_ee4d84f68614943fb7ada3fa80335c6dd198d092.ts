import { Component, OnInit } from '@angular/core';
import { Observable } from 'rxjs';
import { UrlDownloadService } from '../../service/url-download.service';
import { Person } from '../../types/person';

@Component({
  selector: 'app-person',
  templateUrl: './person.component.html',
  styleUrls: ['./person.component.css']
})
export class PersonComponent implements OnInit {

  person: Person = {
    id: 1,
    firstName: 'Gagik',
    lastName: 'Gagyan',
    imageUrl: 'https://files.ams3.digitaloceanspaces.com/media/users/5a40d7ec0d9db41a04dfd663/avatars/180x180.JPEG',
    image: new Observable()
  };

  image: Observable<any>;

  fetchImage(url: string): Observable<Blob> {
    return this.urlDownloadService.fetch(url);
  }

  constructor(
    private urlDownloadService: UrlDownloadService
  ) { }

  ngOnInit() {
    this.image = this.urlDownloadService.fetch(this.person.imageUrl);
    this.image.subscribe(data => console.log(data));
  }
}