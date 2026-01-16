import { Types, kernel } from "../dependency-injection/";
import { ValidationException } from "../exceptions/";
import * as Repositories from '../repositories/';
import * as Entities from '../entities/';
import { ActionBase } from './ActionBase';
import { ActionContext } from './ActionBase';
import * as Exceptions from '../exceptions';
import { IBotService } from '../services/';
import { IIotPlatform } from '../providers/';

export class Action extends ActionBase<Entities.Bot[]> 
{
    private _boxRepository: Repositories.BoxRepository;
    private _botRepository: Repositories.BotRepository;
    private _botService: IBotService;

    constructor() 
    {
        super();
        this._boxRepository = kernel.get<Repositories.BoxRepository>(Types.BoxRepository);
        this._botRepository = kernel.get<Repositories.BotRepository>(Types.BotRepository);
        this._botService = kernel.get<IBotService>(Types.BotService);
    };

    protected getConstraints() 
    {
        return {}
    }

    protected getSanitizationPattern() 
    {
        return {};
    }

    public async execute(context: ActionContext): Promise<Entities.Bot[]> {

        let bots: Entities.Bot[] = await this._botRepository.findAll();

        for (let i = 0; i < bots.length; i++) {
            await this._botService.activate(bots[i]);
        }

        return bots;
    }
}