import { AfterViewInit, Component, OnInit, ViewChild } from '@angular/core';
import { NgForm } from '@angular/forms';
import {
  MatDialog,
  MatDialogRef,
  MAT_DIALOG_DATA,
} from '@angular/material/dialog';
import { MatPaginator } from '@angular/material/paginator';
import { MatTableDataSource } from '@angular/material/table';
import { CadastroProdutoComponent } from '../cadastro/cadastro-produtos.component';
import { Produto } from '../model/ProdutoInterface';
import { ProdutoService } from '../service/produto.service';

@Component({
  selector: 'app-produtos',
  templateUrl: './produtos.component.html',
  styleUrls: ['./produtos.component.css'],
})
export class ProdutosComponent implements OnInit, AfterViewInit {
  constructor(public dialog: MatDialog, private service: ProdutoService) {}

  displayedColumns: string[] = [
    'nome',
    'preco',
    'ativo',
    'materia_prima',
    'acoes',
  ];

  dataSource = new MatTableDataSource<Produto>([]);

  @ViewChild(MatPaginator) paginator?: MatPaginator;

  ngOnInit(): void {
    this.getAllProdutos();
  }

  ngAfterViewInit() {
    this.dataSource.paginator = this.paginator!;
  }

  private getAllProdutos(): void {
    this.service.getAllProdutos().subscribe((produtos: Produto[]) => {
      this.dataSource = new MatTableDataSource<Produto>(produtos);
    });
  }

  novoProduto(): void {
    let produto = {} as Produto;
    this.createOrEditProduto(produto);
  }

  private createOrEditProduto(produto: Produto): void {
    const dialogRef = this.dialog.open(CadastroProdutoComponent, {
      disableClose: true,
      data: produto,
    });

    dialogRef.afterClosed().subscribe((result) => {
      let produto = result as Produto;

      if (produto === undefined) return;

      if (produto.id !== undefined) {
        this.updateProduto(produto);
      } else {
        this.saveProduto(produto);
      }
    });
  }

  saveProduto(produto: Produto) {
    this.service.saveProduto(produto).subscribe(() => {
      this.getAllProdutos();
    });
  }

  updateProduto(produto: Produto) {
    this.service.updateProduto(produto).subscribe(() => {
      this.getAllProdutos();
    });
  }

  edit(produto: Produto) {
    this.createOrEditProduto(produto);
  }

  delete(produto: Produto) {
    this.service.deleteProduto(produto).subscribe(() => {});
  }
}