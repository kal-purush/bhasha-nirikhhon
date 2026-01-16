import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http'

@Injectable({
  providedIn: 'root'
})
export class CompteService {

  constructor(private readonly http: HttpClient) { }

  connexion(login: string, password: string): Promise<any> {
    let url = "http://localhost:3000/utilisateur/{login}/{password}"
    url = url.replace("{login}", login)
    url = url.replace("{password}", password)
    return this.http.get<any>(url)
    .toPromise()
    .then((res) => {
      if(res == null) {
        return false
      }
      return true
    }).catch((e) => {
      console.log("catch")
      console.log(e)
      return undefined
    })
  }
}