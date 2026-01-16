import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { Subject } from 'rxjs';
import { switchMap, map, debounceTime, distinctUntilChanged, takeUntil } from 'rxjs/operators';
import { Customer } from './customer.model';
import { CustomersService } from './customers.service';

@Component({
  selector: 'app-customer-search',
  templateUrl: './customer-search.component.html',
  styleUrls: ['./customer-search.component.scss'],
})
export class CustomerSearchComponent implements OnInit, OnDestroy {
  // export class CustomerSearchComponent extends BootstrapBaseComponent implements OnInit, OnDestroy {
  title = 'switchMap-example';

  customerFormGroup: FormGroup;
  customer: Customer;
  customersWithSwitchmap: Customer[] = [];
  requestsWithSwitchmap = [];
  customersWithoutSwitchmap = [];
  // requestsWithoutSwitchmap = [];
  private customerLookup$: Subject<void> = new Subject();
  private distroyed$: Subject<boolean> = new Subject();

  constructor(private fb: FormBuilder, private customersService: CustomersService) {
    // super();
  }

  ngOnInit() {
    this.customerFormGroup = this.fb.group({
      name: '',
      address: '',
      city: '',
      state: '',
      zip: '',
      delayTime: 2000,
    });

    this.customerFormGroup.valueChanges.subscribe(() => {
      this.customerLookup$.next();
      // this.getCustomers();
    });

    this.customerLookup$
      .pipe(
        map(() => (this.requestsWithSwitchmap = [])),
        debounceTime(500),
        distinctUntilChanged(),
        switchMap(() => {
          this.requestsWithSwitchmap.push(this.customerFormGroup.value);
          const searchParams = this.customerFormGroup.value; // add logic before api call here
          return this.customersService.get(searchParams);
        }),
        takeUntil(this.distroyed$)
      )
      .subscribe((results) => {
        console.log(`subscribe .... results=`, results);
        this.customersWithSwitchmap = results; // add logic after api call here});
        this.customersWithSwitchmap = <Customer[]>JSON.parse(JSON.stringify(results)); // add logic after api call here});
        this.requestsWithSwitchmap.shift();
      });

    this.customerLookup$.next();
    // this.getCustomers();
  }

  // getCustomers() {
  //   this.requestsWithoutSwitchmap.push(this.customerFormGroup.value);
  //   const searchParams = this.customerFormGroup.value;
  //   this.customersService.get(searchParams).subscribe((results) => {
  //     this.customersWithoutSwitchmap = results;
  //     this.requestsWithoutSwitchmap.shift();
  //   });
  // }

  ngOnDestroy(): void {
    this.distroyed$.next(true);
    this.distroyed$.complete();
  }
}