import {Injectable} from "@angular/core";
import {Observable} from "rxjs";
import {Http, Response, RequestOptions, Headers} from "@angular/http";
@Injectable()
export class AppService {
  constructor(private http: Http) {
  }

  private general;
  private business;
  private entertainment;
  private technology;
  private sport;
  private gaming;
  private music;
  private science;

  private generalUrl = "http://localhost:3000/general";
  private businessUrl = "http://localhost:3000/business";
  private entertainmentUrl = "http://localhost:3000/entertainment";
  private technologyUrl = "http://localhost:3000/technology";
  private sportUrl = "http://localhost:3000/sport";
  private gamingUrl = "http://localhost:3000/gaming";
  private musicUrl = "http://localhost:3000/music";
  private scienceUrl = "http://localhost:3000/science-and-nature";

  fetchUrl() {
    Observable.forkJoin(
      this.http.get(this.url("associated-press")).map((res) => res.json()),
      this.http.get(this.url("bloomberg")).map((res) => res.json()),
      this.http.get(this.url("mashable")).map((res) => res.json()),
      this.http.get(this.url("the-verge")).map((res) => res.json()),
      this.http.get(this.url("bbc-sport")).map((res) => res.json()),
      this.http.get(this.url("polygon")).map((res) => res.json()),
      this.http.get(this.url("mtv-news")).map((res) => res.json()),
      this.http.get(this.url("national-geographic")).map((res) => res.json())
    ).subscribe(
      data => {
        this.general = data[0];
        this.business = data[1];
        this.entertainment = data[2];
        this.technology = data[3];
        this.sport = data[4];
        this.gaming = data[5];
        this.music = data[6];
        this.science = data[7];
        this.saveDb();
        console.log(data);
      },
      err => console.error(err)
    );

  }

  saveDb() {
    let headers = new Headers({'Content-Type': 'application/json'});
    let options = new RequestOptions({headers: headers});
    let _data = [this.general, this.business, this.entertainment,
      this.technology, this.sport, this.gaming, this.music, this.science];

    let _dataUrl = [this.generalUrl, this.businessUrl, this.entertainmentUrl,
      this.technologyUrl, this.sportUrl, this.gamingUrl, this.musicUrl, this.scienceUrl];
    _data.map((data, i) => {
      data.articles.map(
        article => {
          this.http.post(_dataUrl[i], article, headers).subscribe(
            data => {
              console.log(data)
            },
            error => {
              console.log(error)
            },
            () => {
              console.log('success')
            }
          )
        }, this
      )
    }, this);
  }

  private url(source: string) {
    let prefix = "https://newsapi.org/v1/articles?";
    let sort = "&sortBy=top";
    let api = "&apiKey=b1a9eb71513f43d49c154dd48427a833";
    let sourceStr = "source=" + source;
    let urlString = prefix + sourceStr + sort + api;
    return urlString;
  }
}