import { Request, Response } from "express";
import { parseISO } from 'date-fns'

import AppointmentsRepository from '../repositories/AppointmentsRepository'
import CreateAppointmentService from '../services/CreateAppointmentService';

const appointmentsRepository = new AppointmentsRepository

class AppointmentsController {

    getAll (request: Request, response: Response) {
        const appointments = appointmentsRepository.all()

        return response.json(appointments);
    }

    createOne (request: Request, response: Response) {
        try{
            const { provider, date } = request.body
            const parsedDate = parseISO(date)
            const appointmentService = new CreateAppointmentService(appointmentsRepository)
            const appointment = appointmentService.execute({ provider, date: parsedDate })
    
            return response.json(appointment)
        } catch(error) {
            return response.status(400).json({ error: error.message })
        }
    }
}

export default AppointmentsController