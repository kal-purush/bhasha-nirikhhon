import { Component } from '@angular/core';
import { FormControl, FormsModule, ReactiveFormsModule } from '@angular/forms';


@Component({
  selector: 'app-inicio',
  standalone: true,
  imports: [FormsModule, ReactiveFormsModule, FormsModule ],
  templateUrl: './inicio.component.html',
  styleUrl: './inicio.component.css'
})


export class InicioComponent {

  searchControl = new FormControl('');
  
  onSearch() {
    const query = this.searchControl.value;
    console.log('Búsqueda:', query); // Puedes reemplazar esto con la lógica de búsqueda que desees
  }
  
}