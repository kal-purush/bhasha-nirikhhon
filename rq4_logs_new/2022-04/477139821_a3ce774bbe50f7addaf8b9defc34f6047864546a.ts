import { Injectable } from '@nestjs/common';
import { Repository } from 'typeorm';
import { Menu } from '../entities/Menu';
import { InjectRepository } from '@nestjs/typeorm';
import { DetailMenu } from '../entities/DetailMenu';

@Injectable()
export class DetailService {
    constructor(
        @InjectRepository(DetailMenu)
        private  detailRepository: Repository<DetailMenu>,
    )   {}

        async getDetailMenu(id:string) : Promise<any> {
            return this.detailRepository.findOne({idMenu:id});
        }

}