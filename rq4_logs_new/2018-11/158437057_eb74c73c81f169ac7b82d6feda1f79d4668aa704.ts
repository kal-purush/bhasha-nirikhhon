import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GridComponent } from './grid.component';
import { Tile } from '../tile/tile.model';
import { Component, Input, OnInit } from '@angular/core';

describe('GridComponent', () => {
  let renderedRows:Tile[][];
  @Component({
    selector: 'aar-tile-row',
    template: ''
  })
  class TileRowComponentStub implements OnInit{
    ngOnInit(): void {
      renderedRows.push(this.tiles);
    }

    @Input() tiles:Tile[];
  }

  let component: GridComponent;
  let fixture: ComponentFixture<GridComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ GridComponent, TileRowComponentStub ]
    });

    fixture = TestBed.createComponent(GridComponent);
    component = fixture.componentInstance;
    renderedRows = [];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should renderinput:ed rows (in order)', () => {
    const firstRow = [new Tile("row 0 tile 0"), new Tile("row 0 tile 1")];
    const secondRow = [new Tile("row 1 tile 0"), new Tile("row 1 tile 1")];
    component.rows =  [
      firstRow,
      secondRow
    ]
    fixture.detectChanges();
    expect(renderedRows[0]).toEqual(firstRow);
    expect(renderedRows[1]).toEqual(secondRow);
  });
});