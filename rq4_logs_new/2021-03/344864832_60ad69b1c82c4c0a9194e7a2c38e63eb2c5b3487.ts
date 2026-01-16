import express from 'express';
import { labelService } from '../services/labelService';
import { LabelModel } from '../models/Label';

class LabelController {
    async createLabel(req: express.Request, res: express.Response) {
        const name = req.body.name as string;
        if (!name || !name.trim()) {
            return res.status(400).send({
                type: 'ERROR',
                message: 'Invalid post data'
            });
        }

        try {
            const label = await labelService.createLabel({name} as LabelModel);
            return res.status(200).send({
                id: label._id,
                name
            })
        } catch (e) {
            return res.status(500).send({
                type: 'ERROR',
                message: 'Error occurs during post creation'
            });
        }
    }

    async updateLabel(req: express.Request, res: express.Response) {
        const name = req.body.name as string;
        const labelId = req.headers.id as string;

        if (!name || !name.trim()) {
            return res.status(400).send({
                type: 'ERROR',
                message: 'Invalid post data'
            });
        }

        try {
            await labelService.updateLabel(labelId, {name} as LabelModel);
            return res.status(200).send({
                id: labelId,
                name
            })
        } catch (e) {
            return res.status(500).send({
                type: 'ERROR',
                message: 'Error occurs during post creation'
            });
        }

    }

    async readLabels(req: express.Request, res: express.Response) {
        return labelService.getAllLabels();
    }

    async deleteLabel(req: express.Request, res: express.Response) {
        const labelId = req.headers.id as string;
        return labelService.deleteLabel({_id: labelId});
    }
}

const labelController = new LabelController();

export default labelController;