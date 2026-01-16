import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TileRowComponent } from './tile-row.component';
import { Input, Component, EventEmitter, OnInit } from '@angular/core';
import { Tile } from '../tile/tile.model';
import { inspectNativeElement } from '@angular/platform-browser/src/dom/debug/ng_probe';

describe('TileRowComponent', () => {
  let renderedTiles:Tile[];
  @Component({
    selector: 'aar-tile',
    template: ''
  })
  class TileComponentStub implements OnInit{
    ngOnInit(): void {
      renderedTiles.push(this.tile);
    }

    @Input() tile:Tile;
  }

  let component: TileRowComponent;
  let fixture: ComponentFixture<TileRowComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ TileRowComponent, TileComponentStub ]
    })
    fixture = TestBed.createComponent(TileRowComponent);
    component = fixture.componentInstance;
    renderedTiles = [];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render Input:ed Tiles', () =>{
    const inputTile = new Tile("foobar");
    component.tiles = [inputTile];
    fixture.detectChanges();
    expect(renderedTiles[0]).toEqual(inputTile);
  });

  it('should render tiles in order', () => {
    const inputTiles = [
      new Tile("foobar"),
      new Tile("Hello world"),
      new Tile("Apapa"),
      new Tile("nisse")
    ];
    component.tiles = inputTiles;
    fixture.detectChanges();
    renderedTiles.forEach((rendered, index) => 
                    expect(rendered).toEqual(inputTiles[index]));
  });
});