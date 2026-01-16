import { Component, OnInit } from '@angular/core';
import { DataService } from '../../service/data.service';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Vehicule } from '../../models/vehicule';

@Component({
  selector: 'app-cycle-vie-vehicule',
  templateUrl: './cycle-vie-vehicule.component.html'
})
export class CycleVieVehiculeComponent implements OnInit {

  vehicule: Vehicule = new Vehicule('','',[],'','',undefined);
  immatriculation: string;

  constructor(private _srv:DataService, private route: ActivatedRoute) {
    this.immatriculation = route.snapshot.paramMap.get('immatriculation');
  }

  ngOnInit() {

    //abonnement au changement de route avec réutilisation du composant par le router
    this.route.paramMap.subscribe((params: ParamMap) => {
      const immatriculation = params.get('immatriculation');
    });

    this._srv.publish(this.immatriculation).subscribe(
      returnValue => this.vehicule = returnValue
    );

  }

}