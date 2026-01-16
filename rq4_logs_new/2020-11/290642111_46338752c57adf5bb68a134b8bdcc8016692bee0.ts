import { Component, Inject, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { PostempService } from '../services/postemp.service';
import Swal from 'sweetalert2';



@Component({
  selector: 'app-post-empresas',
  templateUrl: './post-empresas.component.html',
  styleUrls: ['./post-empresas.component.css']
})
export class PostEmpresasComponent implements OnInit {
  postempForm: FormGroup
  constructor(
    private _builder: FormBuilder,
    public postempSvc: PostempService,
    private dialogRef: MatDialogRef<PostEmpresasComponent>,
    @Inject(MAT_DIALOG_DATA) data) { 

      this.postempForm = this._builder.group({
        titlePost:['',Validators.required],
        contentPost:['',Validators.required],
        tagsPost:['',Validators.required],
      })
    }

  ngOnInit(): void {
  }

  onSaveForm(){
  /* METODO Y ALERT AGREGACIÓN */
    if(this.postempSvc.selected.id == null){
      let newPostemp = {
        titlePost: this.postempSvc.selected.titlePost,
        contentPost: this.postempSvc.selected.contentPost,
        tagsPost: this.postempSvc.selected.tagsPost,
      }
      Swal.fire('Proyecto publicado','Su proyecto ha sido publicado con éxito.', 'success')
      this.postempSvc.addPostemp(newPostemp);
    }
    else{
  /* METODO Y ALERT EDICIÓN */
      Swal.fire('Proyecto editado','Su proyecto ha sido editado con éxito.', 'success')
      this.postempSvc.editPostemp(this.postempSvc.selected);
    }
    this.close();
  }
 /*  CLASE PARA CERRAR EL POPUP */
  close(): void{
    this.dialogRef.close();
  }

}

