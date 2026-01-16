import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { tap, delay, take } from "rxjs/operators";
import { Funcionario } from "./funcionarios";

@Injectable({
  providedIn: "root"
})
export class FuncionariosService {
  private readonly API = "http://localhost:8080/funcionarios";

  constructor(private http: HttpClient) {}

  find() {
    const response = this.http
      .get<Funcionario[]>(this.API)
      .pipe(delay(2000), tap());
    return response;
  }

  findById(id) {
    return this.http.get<Funcionario>(`${this.API}/${id}`).pipe(take(1));
  }

  private create(funcionario) {
    return this.http.post(this.API, funcionario);
  }

  private update(funcionario) {
    return this.http
      .put(`${this.API}/${funcionario.id}`, funcionario)
      .pipe(take(1));
  }

  delete(id) {
    return this.http.delete(`${this.API}/${id}`).pipe(take(1));
  }

  save(funcionario) {
    if (funcionario.id) {
      return this.update(funcionario);
    } else {
      return this.create(funcionario);
    }
  }
}