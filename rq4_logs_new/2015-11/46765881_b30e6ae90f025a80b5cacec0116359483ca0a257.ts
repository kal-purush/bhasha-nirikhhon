import express = require('express');
import velocity = require('velocity');

import {Get, Post, Put, Delete, Controller} from "../../../../lib/router/router";

import {ITabela} from "../../tabela/model/ITabela";
import {ITabelaCampo} from "../../tabela/model/ITabelaCampo";

import {TabelaCampo} from "../../tabela/controller/TabelaCampo";


@Controller()
export class Gerador {
	@Get("test")
	getTest(req: express.Request, res: express.Response): void {
		//velocity.en
		var Engine = velocity.Engine;
		//var t = new Engine("");
		
		var engine = new Engine({
			template: './app/br/underdesk/gerador/resource/template/teste1.vm'
			, output: './teste_save1.txt'
		});

		var rst:string = engine.render({ nome: "paloma" });

		res.send(rst);
		
	}

	@Post()
	get(req: express.Request, res: express.Response): void {

		var tmpTabela: ITabela[] = <ITabela[]>req.body;

		var tmpTabCampCtrl: TabelaCampo = new TabelaCampo();

		var ctotal: number = 0;

		tmpTabela.forEach(function(tmpItemTab:ITabela){
			tmpTabCampCtrl.getByIdTabela(tmpItemTab.id).then(function(dta: ITabelaCampo[]) {
				tmpItemTab.campo = dta;
				ctotal++;
				if (ctotal == tmpTabela.length) {
					var engine = new velocity.Engine({
						template: './app/br/underdesk/gerador/resource/template/UML.vm'
						, output: './teste_uml.violet.html'
					});
					var rst: string = engine.render({
						classes: tmpTabela
					});
					res.send(tmpTabela);
				}
			}).catch(function(err: any) {
				res.status(400).json(err);
			});
		});
	}
}