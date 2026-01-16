import { Request, Response } from "express";
import { z } from "zod";
import logger from "@root/logger";
import { extractErrorsFromZod } from "@root/utils";
import { AppDataSource } from "@root/data-source";
import Feedback from "@root/entity/Feedback";
import Pizza from "@root/entity/Pizza";
import Invoice from "@root/entity/Invoice";
import NUMBER from "@root/schemas/Number";

const FeedbackRepository = AppDataSource.getRepository(Feedback);
const PizzaRepository = AppDataSource.getRepository(Pizza);
const InvoiceRepository = AppDataSource.getRepository(Invoice);

const CreateFeedbackParams = z.object({
    pizzaId: NUMBER,
    invoiceId: NUMBER,
    feedback: z.string(),
});

export default async function createFeedback(req: Request, res: Response) {
    const parsed = CreateFeedbackParams.safeParse(req.body);
    if (parsed.error) {
        logger.warn(parsed.error);
        res.BadRequest(extractErrorsFromZod(parsed.error));
        return;
    }

    const pizzaId = parsed.data.pizzaId;
    const invoiceId = parsed.data.invoiceId;
    const feedback = parsed.data.feedback;

    try {
        const pizza = await PizzaRepository.findOne({
            where: {
                id: pizzaId
            }
        })

        if (!pizza) {
            res.BadRequest([{ message: `Pizza with ID ${pizzaId} not found`}]);
            return;
        }

        const invoice = await InvoiceRepository.findOne({
            where: {
                id: invoiceId
            }
        })

        if (!invoice) {
            res.BadRequest([{ message: `Invoice with ID ${invoiceId} not found`}]);
            return;
        }

        const existingFeedback = await FeedbackRepository.findOne({
            where: {
                pizza: { id: pizzaId },
                invoice: { id: invoiceId }
            }
        });

        if (existingFeedback) {
            res.BadRequest([{ message: `Feedback for Pizza ID ${pizzaId} and Invoice ID ${invoiceId} already exists.` }]);
            return;
        }

        const createFeedback = new Feedback();
        createFeedback.pizza = pizza;
        createFeedback.invoice = invoice;
        createFeedback.feedback = feedback;

        await FeedbackRepository.save(createFeedback);

        res.Ok(createFeedback);
    } catch (e) {
        logger.error(e);
        res.InternalServerError(e);
        return;
    }
}