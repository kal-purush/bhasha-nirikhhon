import { LojaBranchsService } from './loja-branchs.service';
import { CreateLojaBranchDto } from './dto/create-loja-branch.dto';
import { UpdateLojaBranchDto } from './dto/update-loja-branch.dto';
export declare class LojaBranchsController {
    private readonly lojaBranchsService;
    constructor(lojaBranchsService: LojaBranchsService);
    create(createLojaBranchDto: CreateLojaBranchDto): Promise<import("./entities/loja-branch.entity").LojaBranch>;
    findAll(page: number, limit: number, search: string): Promise<import("nestjs-typeorm-paginate").Pagination<import("./entities/loja-branch.entity").LojaBranch, import("nestjs-typeorm-paginate").IPaginationMeta>>;
    findOne(id: number): Promise<import("./entities/loja-branch.entity").LojaBranch>;
    update(id: number, updateLojaBranchDto: UpdateLojaBranchDto): Promise<import("./entities/loja-branch.entity").LojaBranch>;
    remove(id: number): Promise<boolean>;
}