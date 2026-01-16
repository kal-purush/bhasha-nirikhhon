import { Request, Response } from 'express';
import { Estimate } from '../models/Estimate';


export class EstimateController {
    static getEstimate = async (req: Request, res: Response) => {
        try {
            const allEstimate: any = await Estimate.find({}).populate('clientId');
            if (!allEstimate) throw { code: 400 }
            res.status(200).send({ error: true, Devis: allEstimate });
        } catch (err) {
            if (err.code === 400) res.status(401).send({ error: true, message: 'Il n\'y a pas de Devis' });
        }
    }
    static getOneEstimate = async (req: Request, res: Response) => {
        try {
            const { id } = req.params;
            const estimate: any = await Estimate.findOne({ _id: id }).populate('clientId');
            if (!estimate) throw { code: 400 }
            res.status(201).send({ error: false, Message: "Devis numéro : " + estimate.estimateNum, estimate });
        } catch (err) {
            if (err.code === 400) res.status(401).send({ error: true, message: 'le devis n\'existe pas' });
        }
    }
    static getEstimatebyClient = async (req: Request, res: Response) => {
        try {
            const { id } = req.params;
            const allEstimateByClient: any = await Estimate.find({ clientId: id }).populate('clientId');
            if (!allEstimateByClient) throw { code: 400 }
            res.status(200).send({ error: true, Estimate: allEstimateByClient });
        } catch (err) {
            if (err.code === 400) res.status(401).send({ error: true, message: 'Il n\'y a pas de devis' });
        }
    }
    static deleteEstimate = async (req: Request, res: Response) => {
        try {
            const { id } = req.params;
            const estimate: any = await Estimate.findOne({ _id: id });
            if (!estimate) throw { code: 400 };
            await Estimate.deleteOne(estimate);
            res.status(201).send({ error: false, Message: "Devis supprimer avec sucess" });
        } catch (err) {
            if (err.code === 400) res.status(401).send({ error: true, message: 'Le devis n\'existe pas' });
        }
    }
}