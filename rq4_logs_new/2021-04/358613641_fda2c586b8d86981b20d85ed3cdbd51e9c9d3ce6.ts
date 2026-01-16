import { Component, Inject, OnInit } from '@angular/core';
import { AbstractControl, FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Ingredient } from '../../models/ingredient.interface';

@Component({
  selector: 'app-ingredient-dialog',
  templateUrl: './ingredient-dialog.component.html',
  styleUrls: ['./ingredient-dialog.component.scss']
})
export class IngredientDialogComponent implements OnInit {
  public ingredientForm!: FormGroup;

  // Controls to be used in template
  public get name(): AbstractControl { return this.ingredientForm.controls.name; }
  public get unit(): AbstractControl { return this.ingredientForm.controls.unit; }
  public get price(): AbstractControl { return this.ingredientForm.controls.price; }

  constructor(
    public dialogRef: MatDialogRef<IngredientDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: Ingredient,
    private formBuilder: FormBuilder
  ) { }

  ngOnInit(): void {
    this.ingredientForm = this.formBuilder.group({
      name: new FormControl('', [Validators.required]),
      unit: new FormControl('', [Validators.required]),
      price: new FormControl('', [Validators.required]),
    });

    if (this.data) {
      // this.ingredientForm.setValue(this.data);
    }
  }

  public onNoClick(): void {
    this.dialogRef.close();
  }

  public onSubmit(): void {
    if (this.ingredientForm.valid) {
      this.dialogRef.close(this.ingredientForm.value);
    }
  }

}