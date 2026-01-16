import { Component, OnInit } from '@angular/core';
import { ServerService} from '../server.service';
import { Router, ActivatedRoute } from '@angular/router';
import {Marsupilamis} from 'src/app/interfaces/marsupilamis';
import {FormBuilder, FormControl, FormGroup} from '@angular/forms';

@Component({
  selector: 'app-modif',
  templateUrl: './modif.component.html',
  styleUrls: ['./modif.component.css']
})
export class ModifComponent implements OnInit {
  mars: Marsupilamis[];
  _id: any;

  newmars: FormGroup = new FormGroup({
    _id: new FormControl(''),
    Nom: new FormControl(''),
    famille: new FormControl(''),
    age: new FormControl(''),
    race: new FormControl(''),
    nourriture: new FormControl(''),
    username: new FormControl(''),
    pass: new FormControl(''),
  });

  constructor(private formBuilder: FormBuilder, private route: ActivatedRoute , private marsupilamiService: ServerService) {
  }
  ngOnInit() {
    /*this.route.params.subscribe(params => {
      if (params['id']) {
        this.marsupilamiService.get(params['id'])
          .subscribe(result => {
            console.log(result);
            this.mars.patchValue(result);
          }, failure => {
            console.error(failure);
          });
      }
    });*/

    this.marsupilamiService.getAll()
      .subscribe(result => {
        console.log(result);
        this.mars = result;
      }, failure => {
        console.error(failure);
      });
  }
  onSubmit() {

    if (this.mars.status === 'VALID') {
      this.marsupilamiService.update(
        {
          ...this.mars.value,
        }).subscribe(result => {
        console.log(result);

      }, err => {
        console.error(err);
      });
    } else {
      alert('il y a un erreur');
    }
  }



}