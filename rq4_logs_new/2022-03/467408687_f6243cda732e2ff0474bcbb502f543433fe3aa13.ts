import { Controller, Post, Get, Patch, Delete, Param, Body } from "@nestjs/common";
import { AdminService } from './admin.service';


@Controller('admin')
export class AdminController {
    constructor(private readonly adminService: AdminService) {}

    @Post()
    async addAdmin(
        @Body('name') name:string,
        @Body('phone') phone:string,
        @Body('email') email:string, 
         ) {
         const generatedId = await this.adminService.createAdmin(
             name,
             phone, 
             email);

        return { id:generatedId }
        }


    @Get()
    async getAlladmin() {
        const admin = await this.adminService.getAdmins();
        return admin;
    }

    @Get(':id')
    async getAdmin(@Param('id') AdminId: string) {
       const Admin = await this.adminService.getSingleAdmin(AdminId);
       return Admin;
    }

    @Patch(':id')
    async updateAdmin( 
      @Param('id') AdminId: string,
      @Body('name') name: string,
      @Body('phone') phone: string,
      @Body('email') email: string,
    ) {
      await this.adminService.updateAdmin(AdminId, name, phone, email);
       return null; 
    }

    @Delete(':id')
    async removeAdmin(@Param('id') AdminId:string) { 
     await this.adminService.deleteAdmin(AdminId);
      return null;
    }
    }