import 'rxjs/Rx';

import { Component } from '@angular/core';

@Component({
  selector: 'list-view-feedback',
  templateUrl: './list-view-feedback.component.html',
  styleUrls: ['./grid-feedback.component.scss']
})
export class FeedbackListComponent {

  public items: any [] = [
    {
      id: '1',
        contextmenu: '',
        status: 'Active',
        email: 'coolemail@gmail.com',
        firstname: 'Steve',
        lastname: 'Brush',
        feedback: 'positive',
        dnc: 'false',
        assigned: 'Colleen Gutierrez',
        datereceived: '07/27/2017',
        notes: ''
    },
    {
      id: '2',
        contextmenu: '',
        status: 'Active',
        email: 'coolemail@gmail.com',
        firstname: 'Brandon',
        lastname: 'Jones',
        feedback: 'positive',
        dnc: 'false',
        assigned: 'Denise Pena',
        datereceived: '07/24/2017',
        notes: 'I received the Altru book, and some of my colleagues are quite taken with it and want to steal it, so I was wondering if we are allowed to order more. How many are we allowed to get?'
    },
    {
      id: '3',
        contextmenu: '',
        status: 'Awaiting Response',
        email: 'coolemail@gmail.com',
        firstname: 'John',
        lastname: 'Lyons',
        feedback: 'negative',
        dnc: 'false',
        assigned: 'RoxAnna Fuquay',
        datereceived: '07/19/2017',
        notes: ''
    },
    {
      id: '4',
        contextmenu: '',
        status: 'Active',
        email: 'coolemail@gmail.com',
        firstname: 'Chris',
        lastname: 'Rodgers',
        feedback: 'positive',
        dnc: 'false',
        assigned: 'Bryna Gleich',
        datereceived: '07/17/2017',
        notes: ''
    },
    {
      id: '5',
        contextmenu: '',
        status: 'Closed',
        email: 'coolemail@gmail.com',
        firstname: 'Lindsey',
        lastname: 'Rix',
        feedback: 'positive',
        dnc: 'false',
        assigned: 'Denise Kadilak',
        datereceived: '07/13/2017',
        notes: ''
    },
    {
      id: '6',
        contextmenu: '',
        status: 'Closed',
        email: 'coolemail@gmail.com',
        firstname: 'David',
        lastname: 'Owens',
        feedback: 'negative',
        dnc: 'true',
        assigned: 'Betsy Unger',
        datereceived: '07/05/2017',
        notes: ''
    },
    {
      id: '7',
        contextmenu: '',
        status: 'Closed',
        email: 'coolemail@gmail.com',
        firstname: 'Brandon',
        lastname: 'Hare',
        feedback: 'negative',
        dnc: 'false',
        assigned: 'Justin Ward',
        datereceived: '07/04/2017',
        notes: '' }
  ];
}