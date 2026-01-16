import { Injectable } from '@angular/core';
import Swal from 'sweetalert2';

@Injectable({
  providedIn: 'root'
})

export class AlertaService {
  constructor() {}

  public alertaSucesso(mensagem) {
    return Swal.fire({
        icon: 'success',
        title: mensagem,
        buttonsStyling: false,
        customClass: {
          confirmButton: 'botao',
        }
      });
  }

  public alertaErro(mensagem) {
    return Swal.fire({
        icon: 'error',
        title: mensagem,
        buttonsStyling: false,
        customClass: {
          confirmButton: 'botao',
        }
      });
  }

  public alertaSucessoRecarrega(mensagem) {
    return Swal.fire({
      icon: 'success',
      title: mensagem,
      buttonsStyling: false,
      customClass: {
        confirmButton: 'botao',
      }
    }).then((result) => {
      window.location.reload();
    })
  }
}