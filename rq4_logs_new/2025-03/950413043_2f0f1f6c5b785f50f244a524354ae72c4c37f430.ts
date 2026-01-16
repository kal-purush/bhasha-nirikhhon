import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ProductosService } from '../../services/Productos.service';
import { ProveedorService } from '../../services/Proveedor.service';

@Component({
  selector: 'app-agregar-producto',
  templateUrl: './agregar-producto.component.html',
  styleUrls: ['./agregar-producto.component.css']
})
export class AgregarProductoComponent implements OnInit {

  nuevoProducto = {
    codigoBarras: '',
    nombreProducto: '',
    tamano: '', // Cambié 'tamaño' a 'tamano'
    categoriaMaquillaje: '',
    subcategoria: '',
    marca: '',
    nombreProveedor: '',
    precioCaja: '',
    precioPieza: '',
    cantidadPorCaja: '',
    cantidadPiezas: '',
    stockExhibe: '',
    stockExhibeMin: '',
    stockAlmacen: '',
    stockAlmacenMin: '',
    imagen: '' // Guardar la URL de la imagen
  };
  proveedores: any[] = [];

  constructor(
    private productosService: ProductosService,
    private proveedorService: ProveedorService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.obtenerProveedores();  // Obtener la lista de proveedores
  }

  obtenerProveedores(): void {
    this.proveedorService.getProveedores().subscribe(
      (data) => {
        this.proveedores = data || [];
      },
      (error) => {
        console.error('Error al obtener proveedores', error);
      }
    );
  }

  agregarProducto(): void {
    const formData = {
      codigoBarras: this.nuevoProducto.codigoBarras,
      nombreProducto: this.nuevoProducto.nombreProducto,
      tamano: this.nuevoProducto.tamano,  // Cambié 'tamaño' a 'tamano'
      categoriaMaquillaje: this.nuevoProducto.categoriaMaquillaje,
      subcategoria: this.nuevoProducto.subcategoria,
      marca: this.nuevoProducto.marca,
      nombreProveedor: this.nuevoProducto.nombreProveedor,
      precioCaja: this.nuevoProducto.precioCaja,
      precioPieza: this.nuevoProducto.precioPieza,
      cantidadPorCaja: this.nuevoProducto.cantidadPorCaja,
      cantidadPiezas: this.nuevoProducto.cantidadPiezas,
      stockExhibe: this.nuevoProducto.stockExhibe,
      stockExhibeMin: this.nuevoProducto.stockExhibeMin,
      stockAlmacen: this.nuevoProducto.stockAlmacen,
      stockAlmacenMin: this.nuevoProducto.stockAlmacenMin,
      imagen: this.nuevoProducto.imagen
    };

    this.productosService.registrarProducto(formData).subscribe(
      (data) => {
        console.log('Producto agregado', data);
        this.router.navigate(['/almacen']);  // Redirigir a la lista de productos
      },
      (error) => {
        console.error('Error al agregar producto', error);
      }
    );
  }
}