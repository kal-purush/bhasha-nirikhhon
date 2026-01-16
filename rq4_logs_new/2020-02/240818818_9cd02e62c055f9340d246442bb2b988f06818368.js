import * as Yup from "yup";
import Recipient from "../models/Recipient";

class RecipientController {
	async store(req, res) {
		const schema = Yup.object().shape({
			rua: Yup.string().required(),
			número: Yup.string().required(),
			complemento: Yup.string().required(),
			estado: Yup.string().required(),
			cidade: Yup.string().required(),
			CEP: Yup.string().required()
		});

		if (!(await schema.isValid(req.body))) {
			return res.status(400).json({ error: "Validation failed" });
		}

		Recipient.create(req.body);
	}

	async update(req, res) {
		const { id } = req.params;

		const schema = Yup.object().shape({
			rua: Yup.string().required(),
			número: Yup.string().required(),
			complemento: Yup.string().required(),
			estado: Yup.string().required(),
			cidade: Yup.string().required(),
			CEP: Yup.string().required()
		});

		if (!(await schema.isValid(req.body))) {
			return res.status(400).json({ error: "Validation failed" });
		}

		const recipient = await Recipient.findByPk(id);

		const {
			rua,
			número,
			complemento,
			estado,
			cidade,
			CEP
		} = await recipient.update(req.body);

		return res.json({ id, rua, número, complemento, estado, cidade, CEP });
	}
}

export default new RecipientController();