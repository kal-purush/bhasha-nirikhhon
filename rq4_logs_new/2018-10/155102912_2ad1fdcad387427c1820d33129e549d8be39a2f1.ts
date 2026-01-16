import {List, Record} from 'immutable';


export interface EventInterface {
  start: Date, end: Date, name: string, room: string, category: string,
}

export class Event {
  start: Date;
  end: Date;
  name: string;
  room: string;
  category: string;
}

const eventDefaults = {
  start: new Date().getTime(),
  end: new Date().getTime(),
  name: '',
  room: '',
  category: '',
}

export class EventRecord extends Record
(eventDefaults) {
  // Set the params. This will also typecheck when we instantiate a new
  // EventRecord
  constructor(params: EventInterface) {
    super(params);
  }

  // This following line is the magic. It overrides the "get" method of record
  // and lets typescript know the return type based on our IFruitParams
  // interface
  get<T extends keyof EventInterface>(value: T): EventInterface[T] {
    // super.get() is mapped to the original get() function on Record
    return super.get(value)
  }
}

export let events = List<EventRecord>([
  new EventRecord({
    'end': new Date(104400000 + (1541259000.0 * 1000)),
    'start': new Date(104400000 + (1541257200.0 * 1000)),
    'name': 'All Buses Depart',
    'room': 'ESB',
    'category': 'BUSES'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541111400.0 * 1000)),
    'start': new Date(104400000 + (1541109600.0 * 1000)),
    'name': 'Sleep',
    'room': '300',
    'category': 'SLEEP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541174400.0 * 1000)),
    'start': new Date(104400000 + (1541172600.0 * 1000)),
    'name': 'L3 ForceX workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541151000.0 * 1000)),
    'start': new Date(104400000 + (1541116800.0 * 1000)),
    'name': 'Rm 460',
    'room': '1',
    'category': 'SLEEP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541163600.0 * 1000)),
    'start': new Date(104400000 + (1541161800.0 * 1000)),
    'name': 'BNY Mellon workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541104200.0 * 1000)),
    'start': new Date(104400000 + (1541102400.0 * 1000)),
    'name': 'Opening Ceremony',
    'room': 'Langford',
    'category': 'PRIMARY EVENTS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541248200.0 * 1000)),
    'start': new Date(104400000 + (1541246400.0 * 1000)),
    'name': 'Lunch',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541196000.0 * 1000)),
    'start': new Date(104400000 + (1541194200.0 * 1000)),
    'name': 'Lightning Talks',
    'room': 'Circular room (001)',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541167200.0 * 1000)),
    'start': new Date(104400000 + (1541165400.0 * 1000)),
    'name': 'MicroStrategy workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541111400.0 * 1000)),
    'start': new Date(104400000 + (1541106000.0 * 1000)),
    'name': 'Workshop for Data Science',
    'room': 'Circular room (001)',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541111400.0 * 1000)),
    'start': new Date(104400000 + (1541106000.0 * 1000)),
    'name': 'Workshop for beginners (044/048)',
    'room': '044/048',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541172600.0 * 1000)),
    'start': new Date(104400000 + (1541167200.0 * 1000)),
    'name': 'Cryptography Workshop',
    'room': '044',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541190600.0 * 1000)),
    'start': new Date(104400000 + (1541185200.0 * 1000)),
    'name': 'Travel Reimbursement Office Hours',
    'room': '1st Floor Wond\'ry',
    'category': 'BUSES'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541115000.0 * 1000)),
    'start': new Date(104400000 + (1541113200.0 * 1000)),
    'name': 'Intro to front end',
    'room': '044',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541118600.0 * 1000)),
    'start': new Date(104400000 + (1541116800.0 * 1000)),
    'name': 'Intro to Flask',
    'room': '044',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541118600.0 * 1000)),
    'start': new Date(104400000 + (1541116800.0 * 1000)),
    'name': ' Intro to NodeJS',
    'room': '048',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541118600.0 * 1000)),
    'start': new Date(104400000 + (1541116800.0 * 1000)),
    'name': 'Midnight Snack',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541107800.0 * 1000)),
    'start': new Date(104400000 + (1541106000.0 * 1000)),
    'name': 'Hacking Begins',
    'room': 'VandyHacks',
    'category': 'MLH'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541205000.0 * 1000)),
    'start': new Date(104400000 + (1541203200.0 * 1000)),
    'name': 'Karaoke',
    'room': 'Circular room (001)',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541233800.0 * 1000)),
    'start': new Date(104400000 + (1541196000.0 * 1000)),
    'name': 'Sleep',
    'room': '300',
    'category': 'SLEEP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541237400.0 * 1000)),
    'start': new Date(104400000 + (1541235600.0 * 1000)),
    'name': 'How to Judge',
    'room': 'TBD',
    'category': 'SPNS+MENT/ORGNZR'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541255400.0 * 1000)),
    'start': new Date(104400000 + (1541251800.0 * 1000)),
    'name': 'Closing Ceremony',
    'room': 'SLC',
    'category': 'PRIMARY EVENTS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541104200.0 * 1000)),
    'start': new Date(104400000 + (1541095200.0 * 1000)),
    'name': 'Dinner Sponsored by MicroStrategy',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541149200.0 * 1000)),
    'start': new Date(104400000 + (1541145600.0 * 1000)),
    'name': 'Breakfast Sponsored by L3 ForceX',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541194200.0 * 1000)),
    'start': new Date(104400000 + (1541192400.0 * 1000)),
    'name': 'Cup Stacking',
    'room': 'TBD',
    'category': 'MLH'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541178000.0 * 1000)),
    'start': new Date(104400000 + (1541176200.0 * 1000)),
    'name': 'Red Ventures workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541156400.0 * 1000)),
    'start': new Date(104400000 + (1541154600.0 * 1000)),
    'name': 'Centene workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541205000.0 * 1000)),
    'start': new Date(104400000 + (1541203200.0 * 1000)),
    'name': 'Midnight Snack',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541237400.0 * 1000)),
    'start': new Date(104400000 + (1541235600.0 * 1000)),
    'name': 'Hacking Stops',
    'room': 'VandyHacks',
    'category': 'MLH'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541163600.0 * 1000)),
    'start': new Date(104400000 + (1541160000.0 * 1000)),
    'name': 'Lunch Sponsored by Centene',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541160000.0 * 1000)),
    'start': new Date(104400000 + (1541158200.0 * 1000)),
    'name': 'Tractor Supply Co workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541100600.0 * 1000)),
    'start': new Date(104400000 + (1541093400.0 * 1000)),
    'name': 'Check-In',
    'room': 'Atrium',
    'category': 'PRIMARY EVENTS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541183400.0 * 1000)),
    'start': new Date(104400000 + (1541179800.0 * 1000)),
    'name': 'Dinner Sponsored by Fulcrum GT',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541233800.0 * 1000)),
    'start': new Date(104400000 + (1541232000.0 * 1000)),
    'name': 'Breakfast',
    'room': 'Atrium',
    'category': 'FOOD'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541192400.0 * 1000)),
    'start': new Date(104400000 + (1541187000.0 * 1000)),
    'name': 'Wellness Night - messages/hand spa',
    'room': '044/048',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541244600.0 * 1000)),
    'start': new Date(104400000 + (1541239200.0 * 1000)),
    'name': 'Expo',
    'room': '044/048',
    'category': 'PRIMARY EVENTS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541152800.0 * 1000)),
    'start': new Date(104400000 + (1541151000.0 * 1000)),
    'name': 'BOS Workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541158200.0 * 1000)),
    'start': new Date(104400000 + (1541156400.0 * 1000)),
    'name': 'Unity/VR workshop (044)',
    'room': '044',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541237400.0 * 1000)),
    'start': new Date(104400000 + (1541235600.0 * 1000)),
    'name': 'How to Demo',
    'room': 'TBD',
    'category': 'HACKER EXP'
  }),
  new EventRecord({
    'end': new Date(104400000 + (1541170800.0 * 1000)),
    'start': new Date(104400000 + (1541169000.0 * 1000)),
    'name': 'Fulcrum GT workshop',
    'room': '048',
    'category': 'SPONSOR TALKS'
  }),
]);