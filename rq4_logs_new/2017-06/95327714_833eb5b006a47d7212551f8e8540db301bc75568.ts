import * as Emoji from 'node-emoji';
import * as Moment from 'moment';
import * as _ from 'lodash';
import * as i18n from 'i18next';

export class TimeSlots {
  getTimeSlots() {
    return [
      { id: 1, text: i18n.t('timeslot.reach'), emoji: Emoji.get('clock3') },
      { id: 2, text: i18n.t('timeslot.15min'), emoji: Emoji.get('clock330') },
      { id: 3, text: i18n.t('timeslot.30min'), emoji: Emoji.get('clock930') },
      { id: 4, text: i18n.t('timeslot.45min'), emoji: Emoji.get('clock9') }
    ];
  }
}