
import { HttpResponse } from "@common/http.response";
import { Role } from "@constant/role.enum";
import { BankEntity } from "@data/entity/bank.entity";
import { Public } from "@decorator/public.decorator";
import { Roles } from "@decorator/role.decorator";
import { Body, Controller, Get, HttpCode, HttpStatus, Patch } from "@nestjs/common";
import { ApiBearerAuth, ApiTags } from "@nestjs/swagger";
import { BankService } from "@service/bank/bank.service";

@ApiTags("Bank")
@Controller('api/bank')
export class BankController {
    constructor (
        private bankService: BankService,
    ){}

    @Public()
    @Get('')
    @HttpCode(HttpStatus.OK)
    async getListBank(): Promise<HttpResponse<BankEntity[]>> {
        const listBank = await this.bankService.getListBank();
        return HttpResponse.success(listBank);
    }



    @ApiBearerAuth()
    @Roles(Role.Admin)
    @Patch()
    @HttpCode(HttpStatus.OK)
    async addNewBank(
        @Body() name: string
    ): Promise<HttpResponse<BankEntity>>{
        const newBank = await this.bankService.addNewbank(name);
        return HttpResponse.success(newBank);
    }

}