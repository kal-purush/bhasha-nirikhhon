import { Component, OnInit } from '@angular/core';
import { DatePipe } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';

// Models
import { FactureGlobal } from '../../models/factureGlobal';
import { FactureMois } from '../../models/factureMois';

// Services
import { FactureGlobalService } from '../../service/facture-global.service';
import { FactureMoisService } from '../../service/facture-mois.service';

@Component({
  selector: 'app-facture-accompte',
  templateUrl: './facture-accompte.component.html',
  styleUrls: [ './facture-accompte.component.css' ]
})
export class FactureAccompteComponent implements OnInit {
  id_fact: number;
  factureGlobal: any = {};
  listFactureAccompte: FactureMois[];
  mode: boolean = false;
  message: string;
  messageClass: string;
  factureForm: FormGroup;

  /**
   * Constructor
   * @param activatedRoute request params of routes to get factureGlobal._id
   * @param factureGlobalService facture Global service
   * @param datePipe format date to display in <input type="date"/>
   * @param formBuilder builder used for Reactive forms
   * @param factureMoisService facture mois service
   */
  constructor(
    private activatedRoute: ActivatedRoute,
    private factureGlobalService: FactureGlobalService,
    private factureMoisService: FactureMoisService,
    private datePipe: DatePipe,
    private formBuilder: FormBuilder
  ) {
    this.generateForm()
  }

  getAllFactureAccompte() {
    this.factureMoisService.getAllFactureMois()
      .subscribe(
      factureMois => this.listFactureAccompte = factureMois,
      error => console.log('Error :' + error)
      );
  }

  getAllFactureAccompteByFactureGlobal(id: number) {
    this.factureMoisService.getAllFactureMoisByFactureGlobal(id)
      .subscribe(
      factureMois => this.listFactureAccompte = factureMois,
      error => console.log('Error :' + error)
      );
  }

  onSuccess() {
    this.getAllFactureAccompteByFactureGlobal(this.id_fact);
  }

  /**
   * Generate form
   */
  generateForm() {
    this.factureForm = this.formBuilder.group({
      ref_factureGlobal: [ '', Validators.required ],
      date_creation: [ '', Validators.required ],
      montantHt: [ '', Validators.required ],
      tauxTva: [ '', Validators.required ],
      montantTtc: [ '' ],
      client: [ '', Validators.required ]
    });
  }

  /**
   * OnInit :
   * check if params['id_fact'] set into url.
   * - set this.id_fact = params['id_fact'].
   * - get Devis using this.id_fact.
   */
  ngOnInit() {
    if (this.activatedRoute.snapshot.params[ 'id_fact' ] !== undefined) {
      this.id_fact = this.activatedRoute.snapshot.params[ 'id_fact' ];
      this.getAllFactureAccompteByFactureGlobal(this.id_fact);;
    }
  }

}