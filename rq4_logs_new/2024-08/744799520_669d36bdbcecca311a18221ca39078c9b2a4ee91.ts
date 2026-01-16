import { Body, Controller, Get, Param, Post, Put } from '@nestjs/common';
import { Public } from '../constant';
import { CreateBannerDto } from '../dto/banner/create-banner.dto';
import { UpdateBannerDto } from '../dto/banner/update-banner.dto';
import { BannerService } from '../service/banner.service';

@Controller('banner')
export class BannerController {
    constructor(private readonly bannerService: BannerService) {}

    @Post()
    create(@Body() createBannerDto: CreateBannerDto) {
        return this.bannerService.create(createBannerDto);
    }

    @Public()
    @Get()
    findAll() {
        return this.bannerService.findAll();
    }

    @Put(':id')
    update(@Param('id') id: string, @Body() updateBannerDto: UpdateBannerDto) {
        return this.bannerService.update(id, updateBannerDto);
    }
}