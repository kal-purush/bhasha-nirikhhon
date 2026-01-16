import * as Yup from 'yup';

import Student from '../models/Student';
import HelpOrder from '../models/HelpOrder';

class AnswerController {
  async store(req, res) {
    const { id } = req.params;
    const schema = Yup.object().shape({
      answer: Yup.string().required('Answer is required'),
    });

    if (!(await schema.isValid(req.body))) {
      return res.status(400).json({ error: 'Validation fails' });
    }

    const helpOrder = await HelpOrder.findByPk(id);
    if (!helpOrder) {
      return res.status(400).json({ error: 'Help order does not exist' });
    }

    const student = await Student.findByPk(helpOrder.student_id);
    if (!student) {
      return res.status(400).json({ error: 'Student does not exist' });
    }

    const { answer } = req.body;
    const { question, answer_at } = await helpOrder.update({
      answer,
      answer_at: new Date(),
    });

    return res.json({ question, answer, answer_at, student });
  }
}

export default new AnswerController();