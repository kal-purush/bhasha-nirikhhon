import { Component, Input, OnInit } from "@angular/core";
import { FormArray, FormBuilder, FormControl, FormGroup, Validators } from "@angular/forms";
import { Observable } from "rxjs";
import { filter, startWith, switchMap } from "rxjs/operators";
import { Product } from "src/app/model/product";
import { ProductService } from "src/app/services/product.service";
import { FinancialEvent } from "../model/financial-event";

@Component({
  selector: 'app-add-event',
  templateUrl: './add-event.component.html',
  styleUrls: ['./add-event.component.css']
})
export class AddEventComponent implements OnInit {
  @Input() events: FormArray;
  form: FormGroup;
  products: Observable<Product[]>;
  eventTypes: string[] = ["Expense", "Income","Transfer"];

  constructor(private formBuilder: FormBuilder,
              private productService: ProductService) {}

  ngOnInit(): void {
    this.form = this.buildFormGroup();
    this.loadProducts();
  }

  get type(): FormControl {
    return this.form.controls.type as FormControl;
  }

  get product(): FormControl {
    return this.form.controls.product as FormControl;
  }

  get count(): FormControl {
    return this.form.controls.count as FormControl;
  }

  get price(): FormControl {
    return this.form.controls.price as FormControl;
  }

  get total(): FormControl {
    return this.form.controls.total as FormControl;
  }

  get comment(): FormControl {
    return this.form.controls.comment as FormControl;
  }

  onSubmit(form: FormGroup) {
    console.log("onSubmit: " + JSON.stringify(form.value));
    this.events.push(this.newEvent(form.value));
    this.form = this.buildFormGroup();
  }

  private newEvent(value: FinancialEvent): FormGroup {
    const price = value.type === 'Expense' ? value.price * -1 : value.price;
    
    return this.formBuilder.group({
      type:    new FormControl(value.type),
      id:      new FormControl(value.id),
      product: new FormControl(new Product(value.product)),
      count:   new FormControl(value.count),
      price:   new FormControl(price),
      total:   new FormControl(Math.round(((value.count * price) + Number.EPSILON) * 100) / 100),
      comment: new FormControl(value.comment)
    });
  }

  onBlurCount(event: any) {
    if (event.target.value && !isNaN(event.target.value)) {
      if (!isNaN(this.price.value)) {
        this.total.setValue(this.multiply(event.target.value, this.price.value));
      } else if (!isNaN(this.total.value)) {
        this.price.setValue(this.divide2(this.total.value, event.target.value));
      }
    }
  }

  onBlurPrice(event: any) {
    if (event.target.value && !isNaN(event.target.value)) {
      if (!isNaN(this.count.value)) {
        this.total.setValue(this.multiply(event.target.value, this.count.value));
      } else if (!isNaN(this.total.value)) {
        this.count.setValue(this.divide3(this.total.value, event.target.value));
      }
    }
  }

  onBlurTotal(event: any) {
    if (event.target.value && !isNaN(event.target.value)) {
      if (this.count.value && this.count.value != undefined && !isNaN(this.count.value)) {
        this.price.setValue(this.divide2(event.target.value, this.count.value));
      } else if (this.price.value && this.price.value != undefined && !isNaN(this.price.value)) {
        this.count.setValue(this.divide3(event.target.value, this.price.value));
      }
    }
  }

  displayProduct(product: Product): string {
    return product && product.name ? product.name : '';
  }

  private buildFormGroup(): FormGroup {
    return this.formBuilder.group({
      type:    new FormControl(this.eventTypes[0], Validators.required),
      product: new FormControl(new Product(), Validators.compose([Validators.required])),
      count:   new FormControl(1,     Validators.compose([Validators.required, Validators.min(0.1)])),
      price:   new FormControl(0,     Validators.compose([Validators.required, Validators.min(0.1)])),
      total:   new FormControl(0,     Validators.compose([Validators.required, Validators.min(0.1)])),
      comment: new FormControl()
    });
  }
  private multiply(a: number, b: number): number {
    return Math.round(((a * b) + Number.EPSILON) * 100) / 100;
  }

  private divide2(x: number, y: number): number {
    return Math.round(((x / y) + Number.EPSILON) * 100) / 100;
  }

  private divide3(x: number, y: number): number {
    return Math.round(((x / y) + Number.EPSILON) * 1000) / 1000;
  }

  private loadProducts() {
    this.products = this.form.controls.product.valueChanges.pipe(
      startWith(''),
      filter(res => {
        return res !== null && res.length > 1
      }),
      switchMap((value) => this.filterProduct(value))
    );
  }

  private filterProduct(product: Product): Observable<Product[]> {
    return this.productService.getProductsByName(product.name).pipe(
      filter(data => !!data)
    )
  }
}