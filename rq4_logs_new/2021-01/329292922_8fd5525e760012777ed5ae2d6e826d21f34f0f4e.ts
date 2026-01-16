import { Component, OnInit } from '@angular/core';
import {DataService} from '../data.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-pokemon-main',
  templateUrl: './pokemon-main.component.html',
  styleUrls: ['./pokemon-main.component.scss']
})
export class PokemonMainComponent implements OnInit {
  pokemons = [];

  constructor(private dataService: DataService, private router: Router) { }

  ngOnInit(): void {
    this.getPokemons();
  }

  getPokemons() {
    let pokemonData;
    this.dataService.getPokemons().subscribe((response: any) => {
      response.results.map((result) => {
        this.dataService.getMoreData(result.name)
          .subscribe((uniqRes: any) => {
            pokemonData = {
              id: uniqRes.id,
              name: uniqRes.name,
              types: uniqRes.types,
              url: `https://pokeres.bastionbot.org/images/pokemon/${uniqRes.id}.png`
            };
            this.pokemons.push(pokemonData);
          });
      });
    });
  }
}