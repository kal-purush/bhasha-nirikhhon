import mockData from '../data.json'
import IEvent from '../model/event.model'
import IMockData from '../model/mockData.model'

export default class MockDataHelper {
  static getEvents() {
    const { nextEvent } = mockData as IMockData
    return nextEvent
  }

  static getEventsTitle() {
    const disciplines = this.getEvents().map((event: IEvent) => event.sportTitle)
    // const uniqueDisciplines = [...new Set(disciplines)]
    // const uniqueDisciplines: Array<string> = disciplines.filter(
    //   (discipline, index) => disciplines.indexOf(discipline) === index
    // )
    // return uniqueDisciplines
    return disciplines
  }
}