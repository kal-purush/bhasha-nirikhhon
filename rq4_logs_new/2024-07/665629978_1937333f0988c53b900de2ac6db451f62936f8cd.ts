import { IEvent } from '@/interfaces/Event'
import { mockEvent } from '../utilsTest/mocks'

let eventRepository = [mockEvent]

const getEvent = (id: string) => {
  return eventRepository.find((event) => event.import_id === id)
}

const updateEvent = (data: IEvent, id: string) => {
  eventRepository = eventRepository.map((event) => {
    if (id === event.import_id) {
      return { ...event, ...data }
    }
    return event
  })
}

const reset = () => {
  eventRepository = [mockEvent]
}

export { eventRepository, getEvent, updateEvent, reset }