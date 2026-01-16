import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { WebService } from '../servicios/web.service';
import { PaginadorService } from '../servicios/paginador.service';
import { UserService } from '../servicios/user.service';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { AlertService } from '../servicios/alert.service';
import { FormBuilder, FormGroup, Validators} from '@angular/forms';
import { RutValidator } from '../validaciones/rut';
import { EmailValidator } from '../validaciones/email';
import { CelValidator } from '../validaciones/cel';
declare var swal:any;


@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css'],
  providers: [WebService]
})
export class LoginComponent implements OnInit {

	listaItems;
	paginador: any = {};
	itemsPaginados: any[];
	usuarios:any = null;
	form: FormGroup;
	modo:String;
	modal:any;

  constructor(private router:Router, private user:UserService, private webService:WebService,private paginadorService: PaginadorService,private modalService:NgbModal,private fb:FormBuilder) { }

  ngOnInit() {
  }

  loginUser(e) {
  	e.preventDefault();
  	console.log(e);
  	var username = e.target.elements[0].value;
  	var password = e.target.elements[1].value;
  	 //DE ESTO DESPUES HAY QUE HACER UNA BUSQUEDA POR EL REGISTRO QUE SE TENGA
  	if(username == 'admin' && password == 'admin') {
      //this.user.setUserLoggedIn();
  		this.router.navigate(['home']);
  	}
  }
  //ESTO AUN ESTA EN DESARROLLO, NO CREO SEA LA FORMA CORRECTA DE REGISTRAR
  registrarUser(e) {
	  e.preventDefault();
	  var nombre = e.target.element[0].value;
	  var apellido = e.target.elements[1].value;
	  var rut = e.target.elements[2].value;
	  var telefono = e.target.elements[3].value;
	  var email = e.target.elements[4].value;
  }

  crearUsuario(modal){
		this.modo = 'Crear';
		this.form = this.fb.group({
			nombre:'',
			apellido:'',
			rut:['',RutValidator.verificarRut],
			telefono:['',CelValidator.verificarFormatoCel],
			email:['',EmailValidator.verificarFormatoEmail]
		});
		this.modal = this.modalService.open(modal);
	}

	comprobarRut(rut){
		for(let i=0;i<this.listaItems.length;i++)
			if(this.listaItems[i].rut == rut)
				return false;
		return true;
	}
/*
	DE MOMENTO ESTO NO ES FUNCIONAL POR PROBLEMAS CON WEBSERVICE
	guardarUsuario(usuario){
		if(this.modo==='Crear'){
			if(this.comprobarRut(usuario.rut))
				this.webService.crearUsuario(usuario).subscribe(nuevoUsuario => this.listaItems.push(nuevoUsusario.json()),null,()=>{
					this.modal.close();
					this.setearPagina(this.paginador.paginaActual + ((this.listaItems.length===1 && this.paginador.paginaActual===0)?1:0));
				});
			else
				swal({title: 'Oops...',text: 'Éste rut ya se encuentra registrado',type: 'error',allowOutsideClick: false,allowEscapeKey: false,allowEnterKey: false,showCloseButton: true});
		}
		else {
			usuario._id = this.usuario._id;
			this.webService.modificarFuncionario(usuario).subscribe(()=>this.listaItems[this.listaItems.indexOf(this.usuario)] = usuario,null,()=>{
				this.modal.close();
				this.setearPagina(this.paginador.paginaActual);
			});
		}
	}
*/
}