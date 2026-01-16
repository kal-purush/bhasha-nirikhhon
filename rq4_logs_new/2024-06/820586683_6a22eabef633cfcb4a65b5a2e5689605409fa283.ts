import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { FormBuilder, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { Observable, map, startWith } from 'rxjs';

@Component({
  selector: 'app-multiselect-autocomplete',
  standalone: true,
  imports: [
    CommonModule,
    MatSelectModule,
    MatAutocompleteModule,
    MatCheckboxModule,
    MatSnackBarModule,
    MatInputModule,
    MatFormFieldModule,
    FormsModule,
    ReactiveFormsModule
  ],
  templateUrl: './multiselect-autocomplete.component.html',
  styleUrl: './multiselect-autocomplete.component.scss'
})
export class MultiselectAutocompleteComponent  implements OnInit {
  formulario!: FormGroup;
  @Input() options!: string[];
  @Output() updateForm = new EventEmitter<any>(); // Evento para atualizar o formulário pai
  filteredOptions!: Observable<string[]>;
  selectedOptions: string[] = [];

  constructor(
    private formBuilder: FormBuilder
  ) {}
 
  ngOnInit() {
    this.inicializateAutocomplete();
  }

  inicializateAutocomplete(): void {
    this.formulario = this.formBuilder.group({
      input: ['']
    });

    this.filteredOptions = this.formulario.valueChanges.pipe(
      startWith(''),
      map(value => this.filter(value.input || '')),
    );
  }

  filter(value: string): string[] {
    const filterValue = value.toLowerCase();
    return this.options.filter(option => option.toLowerCase().includes(filterValue));
  }

  selectOption(option: string): void {
    if (this.selectedOptions.includes(option)) {
      this.selectedOptions.splice(this.selectedOptions.indexOf(option), 1);
      this.updateForm.emit(this.selectedOptions);   // Emitindo as opções selecionadas para fora do componente
    } else {
      this.selectedOptions.push(option);
      this.updateForm.emit(this.selectedOptions);   // Emitindo as opções selecionadas para fora do componente
    }
  }

  formatOptions(): string {
    if (this.selectedOptions.length == 0) {
      return 'Selecione uma opção...';
    } else {
      let optionsFormated = '';

      if (this.selectedOptions.length == 1) {
        optionsFormated = this.selectedOptions[0];
      } else {
        optionsFormated = this.selectedOptions[0];
        for (let i = 1; i < this.selectedOptions.length; i++){
          optionsFormated += `, ${this.selectedOptions[i]}`
        }
      }

      return optionsFormated;    
    }
  }

  selectOptionEnter() {
    const optionLowerCase: string = this.formulario.value.input.toLowerCase();
    const optionsLowerCase: string[] = this.options.map((v) => v.toLocaleLowerCase());
    const selectedOptionsLowerCase: string[] = this.selectedOptions.map((v) => v.toLocaleLowerCase());

    if (optionsLowerCase.includes(optionLowerCase)) {
      const index = optionsLowerCase.indexOf(optionLowerCase);
      const optionEnter = this.options[index];

      selectedOptionsLowerCase.includes(optionLowerCase)                          
      ? this.selectedOptions.splice(this.selectedOptions.indexOf(optionEnter), 1) // Se contém: Remove
      : this.selectedOptions.push(optionEnter);                                   // Se não contém: Insere
      
      this.updateForm.emit(this.selectedOptions);   // Emitindo as opções selecionadas para fora do componente
    } else {
      const insertIncompleteValue: boolean[] = optionsLowerCase.map((v) =>  v.includes(optionLowerCase));
      const trueCount = insertIncompleteValue.filter(insertIncompleteValue => insertIncompleteValue === true).length;
      
      if (!insertIncompleteValue.includes(true)) {
        // se não existir a opção digitada no formulário eu dou a opção para o usuário de inserir uma nova...
        this.addNewOption();
      } else {
        if (trueCount == 1) {
          const index: number = insertIncompleteValue.indexOf(true);
          const optionEnter = this.options[index];

          this.selectedOptions.includes(optionEnter)
          ? this.selectedOptions.splice(this.selectedOptions.indexOf(optionEnter), 1) // Se contém: Remove
          : this.selectedOptions.push(optionEnter);                                   // Se não contém: Insere

          this.updateForm.emit(this.selectedOptions);   // Emitindo as opções selecionadas para fora do componente
        }
      }
    }
  }

  addNewOption() {
    this.options.push(this.formulario.value.input);
    this.selectedOptions.push(this.formulario.value.input);

    this.updateForm.emit(this.selectedOptions);     // Emitindo as opções selecionadas para fora do componente
  }

}