package com.levchenko.module2;

import com.levchenko.module2.model.Customer;
import com.levchenko.module2.model.Invoice;
import com.levchenko.module2.service.PersonService;
import com.levchenko.module2.service.ShopService;
import com.levchenko.module2.service.UserInput;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        UserInput.chooseLimit();
        Customer customer = PersonService.createCustomer();
        Customer customer1 = PersonService.createCustomer();
        Customer customer2 = PersonService.createCustomer();
        Customer customer3 = PersonService.createCustomer();
        Customer customer4 = PersonService.createCustomer();
        Customer customer5 = PersonService.createCustomer();
        Customer customer6 = PersonService.createCustomer();
        Invoice invoice = ShopService.createInvoice(customer);
        System.out.println(invoice.toString());
        Invoice invoice1 = ShopService.createInvoice(customer1);
        System.out.println(invoice1.toString());
        Invoice invoice2 = ShopService.createInvoice(customer2);
        System.out.println(invoice2.toString());
        Invoice invoice3 = ShopService.createInvoice(customer3);
        System.out.println(invoice3.toString());
        Invoice invoice4 = ShopService.createInvoice(customer4);
        System.out.println(invoice4.toString());
        Invoice invoice5 = ShopService.createInvoice(customer5);
        System.out.println(invoice5.toString());
        Invoice invoice6 = ShopService.createInvoice(customer6);
        System.out.println(invoice6.toString());
        ShopService.log(customer, invoice);
        ShopService.log(customer1, invoice1);
        ShopService.log(customer2, invoice2);
        ShopService.log(customer3, invoice3);
        ShopService.log(customer4, invoice4);
        ShopService.log(customer5, invoice5);
        ShopService.log(customer6, invoice6);

        ShopData.info(ShopService.customerInvoices);
    }
}