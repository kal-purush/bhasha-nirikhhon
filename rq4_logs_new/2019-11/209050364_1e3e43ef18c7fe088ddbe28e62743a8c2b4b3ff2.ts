import {Component, Inject, OnInit} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogRef} from '@angular/material/dialog';

@Component({
  selector: 'app-agendas-realizadas-dialog',
  templateUrl: './agendas-realizadas-dialog.component.html',
  styleUrls: ['./agendas-realizadas-dialog.component.css']
})
export class AgendasRealizadasDialogComponent implements OnInit {
  constructor(
    public dialogRef: MatDialogRef<AgendasRealizadasDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any,
  ) {}

  ngOnInit() {}

  criaAgendaRealizada(tipo: string, agendaId: string, texto: string, anexo: File) {
    if (texto) {
      this.dialogRef.close({tipo, agendaId, texto, anexo});
    }
  }

  editaAgendaRealizada(tipo: string, agendaRealizadaId: string, texto: string, anexo: File) {
    if (texto) {
      this.dialogRef.close({tipo, agendaRealizadaId, texto, anexo});
    }
  }

  deletaAgendaRealizada(tipo: string, agendaRealizadaId: string) {
    this.dialogRef.close({ tipo, agendaRealizadaId });
  }
}