import { NextFunction, Request, Response } from 'express';
import coinFlipService from '@services/coinflip.service';
import { logger } from '@/utils/logger';

class CoinflipController {
    private coinFlipService = new coinFlipService();

    public getConfig = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
        try {
            const config = await this.coinFlipService.getContractConfig();

            res.status(200).json(config);
        } catch (error) {
            next(error);
        }
    }

    public getMyBets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
        try {
            const address = String(req.params.address);
            const myBets = await this.coinFlipService.getMyBets(address);

            res.status(200).json(myBets);
        } catch (error) {
            next(error);
        }
    }

    public getPendingBets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
        try {
            const skip: number = Number(req.query.skip);
            const limit: number = Number(req.query.limit);
            const pendingBets = await this.coinFlipService.getPendingBets(skip, limit);

            res.status(200).json(pendingBets);
        } catch (error) {
            next(error);
        }
    }
}

export default CoinflipController;