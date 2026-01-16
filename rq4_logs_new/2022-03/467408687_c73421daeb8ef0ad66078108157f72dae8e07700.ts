import { Controller, Post, Get, Patch, Delete, Param, Body } from "@nestjs/common";
import { CustomersService } from './customers.service';


@Controller('customers')
export class CustomersController {
    constructor(private readonly customersService: CustomersService) {}

    @Post()
    async addCustomer(
        @Body('name') name:string,
        @Body('phone') phone:string,
        @Body('email') email:string, 
         ) {
         const generatedId = await this.customersService.createCustomer(
             name,
             phone, 
             email);

        return { id:generatedId }
        }


    @Get()
    async getAllCustomers() {
        const customers = await this.customersService.getCustomers();
        return customers;
    }

    @Get(':id')
    async getCustomer(@Param('id') customerId: string) {
       const customer = await this.customersService.getSingleCustomer(customerId);
       return customer;
    }

    @Patch(':id')
    async updateCustomer( 
      @Param('id') customerId: string,
      @Body('name') name: string,
      @Body('phone') phone: string,
      @Body('email') email: string,
    ) {
      await this.customersService.updateCustomer(customerId, name, phone, email);
       return null; 
    }

    @Delete(':id')
    async removeCustomer(@Param('id') customerId:string) { 
     await this.customersService.deleteCustomer(customerId);
      return null;
    }
    }