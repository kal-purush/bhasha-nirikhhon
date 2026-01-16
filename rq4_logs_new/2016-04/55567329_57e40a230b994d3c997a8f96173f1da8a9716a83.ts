import {Component, OnInit} from "angular2/core";
import {FamiliaServico} from "../../servicos/familia.servico";
import {RouteParams} from "angular2/router";
import {Familia} from "../../modelos/familia";

@Component({
    templateUrl: "app/paginas/familia/familia-detalhes.html"
})

export class FamiliaDetalhesComponent implements OnInit {
    constructor(private _rotaParams: RouteParams, private _familiaServico: FamiliaServico) { }

    public familia: Familia;

    ngOnInit() {
        let id = <number><any>this._rotaParams.get("id");
        this._familiaServico.obterFamilia(id).subscribe(data => this.familia = data, error => console.log(error));
    }
}