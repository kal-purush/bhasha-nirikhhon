// ===========================================================================>> Core Library
import { Controller, Get, Query } from '@nestjs/common';

// ===========================================================================>> Third Party Library

// ===========================================================================>> Custom Library
import { AdminDashboardService } from './service';

@Controller()
export class AdminDashboardController {

    constructor(
        private readonly _service: AdminDashboardService
    ) { }

    @Get()
    async getData(): Promise<any> {
        return await this._service.getData();
    }

}