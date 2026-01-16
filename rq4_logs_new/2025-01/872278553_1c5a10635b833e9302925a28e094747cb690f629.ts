import { Component, Input, WritableSignal, computed, inject, signal } from '@angular/core';
import { Moneda, Producto } from '../../interfaces/producto';
import { Subscription, interval } from 'rxjs';
import { MonedaService } from '../../services/moneda.service';
import { IMAGES_PRODUCTOS } from '../../constants';
import { CarritoService } from '../../services/carrito.service';
import { ProductosService } from '../../services/productos.service';

@Component({
  selector: 'app-producto-carrusel',
  standalone: true,
  imports: [],
  templateUrl: './producto-carrusel.component.html',
  styleUrl: './producto-carrusel.component.css'
})
export class ProductoCarruselComponent {

  @Input({ required: true }) productos!: Producto[];
  currentIndex: WritableSignal<number> = signal(0);
  private autoSlideSubscription?: Subscription;

  constructor() { }

  carritoService = inject(CarritoService);
  productsService = inject(ProductosService);

  precio = computed(() => {
    let idMoneda = this.carritoService.moneda()?.idMoneda;
    if (idMoneda == undefined) idMoneda = 1;
    return this.productsService.getPrecio(true, this.productos[this.currentIndex()]);
  });

  imagen = computed(() => {
    const prod = this.producto();
    if (prod.image_name) {
      console.log(prod.image_name)
      const image = prod.image_name.substring(0, prod.image_name.lastIndexOf('.')).replaceAll(' ', '_')
      return IMAGES_PRODUCTOS + image.replaceAll('&', '_')
    } else {
      return 'descargar.jpg'
    }
  })

  producto = computed(() => {
    const prod = this.productos[this.currentIndex()];
    console.log(prod)

    return prod;
  })

  ngOnInit() {
    this.startAutoSlide();
  }


  ngOnDestroy() {
    if (this.autoSlideSubscription) {
      this.autoSlideSubscription.unsubscribe();
    }

  }

  startAutoSlide() {
    this.autoSlideSubscription = interval(2500).subscribe(() => {
      this.nextSlide();
    });
  }

  stopAutoSlide() {
    if (this.autoSlideSubscription) {
      this.autoSlideSubscription.unsubscribe();
    }
  }

  nextSlide() {
    this.currentIndex.set((this.currentIndex() + 1) % this.productos.length);
    console.log(this.currentIndex())
  }

  prevSlide() {
    this.currentIndex.set((this.currentIndex() - 1 + this.productos.length) % this.productos.length);
    console.log(this.currentIndex())

  }

}