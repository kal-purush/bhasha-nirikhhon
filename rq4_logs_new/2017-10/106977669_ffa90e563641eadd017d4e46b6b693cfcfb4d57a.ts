import { Component, OnInit }  from '@angular/core';
import { HttpClient }   from '@angular/common/http';

@Component({
  selector: 'crt-civilizations',
  templateUrl: './civilizations.component.html',
  styleUrls: ['./civilizations.component.css']
})

export class CRTCivilizationsComponent implements OnInit {
  results: string[];

  constructor(private http: HttpClient) {}

  ngOnInit(): void {
    this.http.get('/api/civilizations').subscribe(data => {
      this.results = data['results'];
    })
  }
}