import { Component, OnInit } from '@angular/core';
import { AppSettings } from 'src/app/shared/app.settings';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { PlazoDistribucion } from 'src/model/plazodistribucion.model';
import { Producto } from 'src/model/producto.model';
import { Clasificacion } from 'src/model/clasificacion.model';
import { TipoServicio } from 'src/model/tiposervicio.model';
import { Proveedor } from 'src/model/proveedor.model';
import { TipoSeguridad } from 'src/model/tiposeguridad.model';
import { Subscription } from 'rxjs';
import { PlazoDistribucionService } from 'src/app/shared/plazodistribucion.service';
import { ProductoService } from 'src/app/shared/producto.service';
import { ClasificacionService } from 'src/app/shared/clasificacion.service';
import { TipoServicioService } from 'src/app/shared/tiposervicio.service';
import { ProveedorService } from 'src/app/shared/proveedor.service';
import { TipoSeguridadService } from 'src/app/shared/tiposeguridad.service';
import { EnvioBloque } from 'src/model/enviobloque.model';
import { Buzon } from 'src/model/buzon.model';
import { BuzonService } from 'src/app/shared/buzon.service';
import { EnvioBloqueService } from 'src/app/shared/enviobloque.service';
import { Guia } from 'src/model/guia.model';
import { EnvioService } from 'src/app/shared/envio.service';
import { CargoPdfService } from 'src/app/shared/cargo-pdf.service';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { AutogeneradoCreadoModalComponent } from '../autogenerado-creado-modal/autogenerado-creado-modal.component';

@Component({
  selector: 'app-generar-bloque',
  templateUrl: './generar-bloque.component.html',
  styleUrls: ['./generar-bloque.component.css']
})
export class GenerarBloqueComponent implements OnInit {

  constructor(
    private plazoDistribucionService: PlazoDistribucionService,
    private productoService: ProductoService,
    private clasificacionService: ClasificacionService,
    private tipoServicioService: TipoServicioService,
    private proveedorService: ProveedorService,
    private tipoSeguridadService: TipoSeguridadService,
    private buzonService: BuzonService,
    private envioBloqueService: EnvioBloqueService,
    private envioService: EnvioService,
    private cargoPdfService: CargoPdfService,
    private modalService: BsModalService
  ) { }

  bloqueForm: FormGroup;
  excelFile: File;
  envioBloque: EnvioBloque = new EnvioBloque();
  buzon: Buzon;
  autogeneradoCreado: string;
  proveedor: Proveedor;
  guia: Guia;
  
  plazosDistribucion: PlazoDistribucion[];
  productos: Producto[];
  clasificaciones: Clasificacion[];
  tiposServicio: TipoServicio[];
  proveedores: Proveedor[];
  tiposSeguridad: TipoSeguridad[];

  plazosDistribucionSubscription: Subscription;
  productosSubscription: Subscription;
  clasificacionesSubscription: Subscription;
  tiposServicioSubscription: Subscription;
  proveedoresSubscription: Subscription;
  tiposSeguridadSubscription: Subscription;
  
  ngOnInit() {
    this.cargarDatosVista();
    this.bloqueForm = new FormGroup({
      'cantidadDocumentos' : new FormControl(""),
      'plazoDistribucion' : new FormControl(null, Validators.required),
      'producto' : new FormControl(null, Validators.required),
      'clasificacion' : new FormControl(null, Validators.required),
      'tipoServicio' : new FormControl(null, Validators.required),
      'proveedor' : new FormControl(null, Validators.required),
      'tipoSeguridad' : new FormControl(null, Validators.required),
      'excel': new FormControl(null, Validators.required),
      'codigoGuia' : new FormControl(null, Validators.required)
    })
  }

  cargarDatosVista(){
    this.plazosDistribucion = this.plazoDistribucionService.getPlazosDistribucion();
    this.productos = this.productoService.getProductos();
    this.clasificaciones = this.clasificacionService.getClasificaciones();
    this.tiposServicio = this.tipoServicioService.getTiposServicio();
    this.proveedores = this.proveedorService.getProveedores();
    this.tiposSeguridad = this.tipoSeguridadService.getTiposSeguridad();
    this.buzon = this.buzonService.getBuzonActual();

    this.plazosDistribucionSubscription = this.plazoDistribucionService.plazosDistribucionChanged.subscribe(
      plazosDistribucion => {
        this.plazosDistribucion = plazosDistribucion;
      }
    )
    this.productosSubscription = this.productoService.productosChanged.subscribe(
      productos => {
        this.productos = productos
      }
    )
    this.clasificacionesSubscription = this.clasificacionService.clasificacionesChanged.subscribe(
      clasificaciones => {
        this.clasificaciones = clasificaciones;
      }
    )
    this.tiposServicioSubscription = this.tipoServicioService.tiposServicioChanged.subscribe(
      tiposServicio => {
        this.tiposServicio = tiposServicio;
      }
    )
    this.proveedoresSubscription = this.proveedorService.proveedoresChanged.subscribe(
      proveedores => {
        this.proveedores = proveedores;
      }
    )
    this.tiposSeguridadSubscription = this.tipoSeguridadService.tiposSeguridadChanged.subscribe(
      tiposSeguridad => {
        this.tiposSeguridad = tiposSeguridad;
      }
    )
  }

  onChangeExcelFile(file: File){
    if (file == null) {
      this.excelFile = null;
      return null;
    }
    this.excelFile = file;
  }

  onSubmit(datosBloque: FormGroup){

    this.envioBloque.buzon = this.buzon;
    this.envioBloque.plazoDistribucion = datosBloque.get('plazoDistribucion').value;
    this.envioBloque.producto = datosBloque.get('producto').value;
    this.envioBloque.clasificacion = datosBloque.get('clasificacion').value;
    this.envioBloque.tipoServicio = datosBloque.get('tipoServicio').value;
    this.envioBloque.tipoSeguridad = datosBloque.get('tipoSeguridad').value;
    this.guia = datosBloque.get('codigoGuia').value;
    this.proveedor = datosBloque.get('proveedor').value;
    this.envioService.registrarEnvio(this.envioBloque, null, this.guia.id, this.proveedor.id).subscribe(
      envioBloque => {
        this.autogeneradoCreado = envioBloque.autogenerado
        setTimeout(() =>{
          this.cargoPdfService.generarPdfBloque(envioBloque, document.getElementById("codebarBloque").children[0].children[0]);
        }, 200);
        let bsModalRef: BsModalRef = this.modalService.show(AutogeneradoCreadoModalComponent, {
          initialState : {
            autogenerado: envioBloque.autogenerado
          }
        });
        this.bloqueForm.reset();
      }
    )

    console.log(this.envioBloque)
    console.log(this.guia)
    console.log(this.proveedor)
  }



}