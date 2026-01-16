import { Http } from '@angular/http'
import { Injectable } from '@angular/core'
import { Agendamento } from '../agendamento/agendamento'
import { Storage } from '@ionic/storage'


@Injectable()
export class AgendamentoService{
    constructor(
        private _http: Http,
        private _storage: Storage
    ){}

    agenda(agendamento:Agendamento){

          let api = `https://aluracar.herokuapp.com/salvarpedido?carro=${agendamento.carro.nome}&preco=${agendamento.valor}&nome=${agendamento.nome}&endereco=${agendamento.endereco}&email=${agendamento.email}&dataAgendamento=${agendamento.data}`;
        return this._http
            .get(api)
            .toPromise()
            .then(() =>  agendamento.confirmado = true, err => console.log(err))
            .then(() => {
                let key = agendamento.email + agendamento.data.substr(0,10);
                return this._storage.set(key, agendamento)
            })
            .then(() => agendamento.confirmado);   
    }
}