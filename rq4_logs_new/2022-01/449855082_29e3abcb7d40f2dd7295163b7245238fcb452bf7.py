""" A script that finds meeting slots in outlook
"""
import sys
import getpass
import datetime
import zoneinfo

import click
import arrow
from InquirerPy import prompt
from InquirerPy.separator import Separator
from InquirerPy import inquirer

LOCAL_TIMEZONE = datetime.datetime.now().astimezone().tzinfo

class OutlookMac():
    def __init__(self):
        from appscript import app, k
        self.outlook = app('Microsoft Outlook')
        self.k = k

    def get_freebusy(self, attendee, start_time, end_time, interval=15):
        """ check freebusy status in all accounts
        """

        start_time = arrow.get(start_time)
        end_time = arrow.get(end_time)
        for account in self.outlook.exchange_account(): 
            try:
                res = self.outlook.query_freebusy ( 
                    account,
                    for_attendees=[attendee],
                    range_start_time=start_time.naive,
                    range_end_time=end_time.naive,
                    interval=interval,
                )
            except:
                continue
            visibilitiy = {}
            attendee_email = res.pop(0)
            current_time = arrow.get(res.pop(0), 'YYYY-MM-DD HH:mm:ss Z')
            while  current_time < end_time:
                if len(res) == 0: break
                name, location, status =  (res.pop(0), res.pop(0), res.pop(0))
                visibilitiy[current_time] = name, location, status
                current_time = current_time.shift(minutes=interval)

            variations = set(list(visibilitiy.values()))
            if variations == {('', '', 'no info')}:
                continue 
            break
        else:
            raise Exception("Not found %r in %s accounts. Check email, Try restart Outlook or use VPN" % (attendee, len(outlook.exchange_account()) ))

        return visibilitiy


class OutlookWin():
    def __init__(self):
        import win32com.client # pip install pywin32
        self.outlook = win32com.client.Dispatch('Outlook.Application')
        self.namespace = self.outlook.GetNamespace("MAPI")

    def get_freebusy(self, attendee, start_time, end_time, interval=15):
        """ check freebusy status in all accounts
        """
        recipient = self.namespace.CreateRecipient(attendee)
        start_time = arrow.get(start_time)
        end_time = arrow.get(end_time)
        print(start_time.datetime,  attendee )
        res = recipient.FreeBusy(start_time.datetime, interval, True)
        visibilitiy = {}
        current_time = start_time
        for item in res:
            status = {
                '4': 'oof', #olWorkingElsewhere
                '3': 'oof',
                '2': 'busy',
                '1': 'tentative',
                '0': 'free',
            }.get(item, 'no info')
            visibilitiy[current_time] = attendee, '', status
            current_time = current_time.shift(minutes=interval)
        return visibilitiy    

    def create_event(self, subject, content, start, end, attendees):
        event = self.outlook.make(
            new=self.k.calendar_event,
            with_properties={
                self.k.subject: subject,
                self.k.content: content,
                self.k.free_busy_status: self.k.busy,
                self.k.start_time: datetime.datetime(start_time.year, start_time.month, start_time.day, start_time.hour, start_time.minute), 
                self.k.end_time: datetime.datetime(end_time.year, end_time.month, end_time.day, end_time.hour, end_time.minute), 
            },
        )
        for email in attendees or []:
            event.make(
                new=self.k.required_attendee,
                with_properties={self.k.email_address: {self.k.address: email}}
            )
        event.open()
        event.activate()

        return event



@click.command()
@click.option('-a', '--attendees', default=None, help='A semicolon separated list of contacts')
@click.option('--start', default='today', help='Begining of the date range to find available slots. Iso date or Arrow Humanised time (ex: "in 30 days"). See https://arrow.readthedocs.io/en/latest/#dehumanize. Default is today. ')
@click.option('--end', default='in 30 days',
              help='The end of the date range to find available slots. Iso date or Arrow Humanised time (ex: "in 30 days"). See https://arrow.readthedocs.io/en/latest/#dehumanize. Default is "in 30 days".')
@click.option('--full/--only-slots', default=False, help='Show slots or a full agenda (default=only slots)')
@click.option('-r', '--rate', default=100, help='Acceptable share of available attendees in percent(%)')
@click.option('--tentative/--no-tentative', default=True, help="Treat tentative meetings as free")
@click.option('-l', '--lenght', default=60, help='Length of the meeting, in minutes. default=60')
@click.option('-tz', '--alternative_tz', default=None, help='A comma separated list of alternative Timezones.')
@click.option('-hr', '--hours', default='0800-1900', help='Find availability between the hours of... Default = "0800-1900". ')
@click.option('-f', '--fmt', default='HH:mm',
              help='Time format in list of available slots. "HH:mm" or "h:mma". Refer to https://arrow.readthedocs.io/en/latest/#supported-tokens. Default "HH:mm".')
def main(attendees, start, end, full, rate, lenght, tentative, alternative_tz, hours, fmt):
    
    if start == 'today':
        start = arrow.get().replace(minute=0, second=0)
    else:
        try:
            start = arrow.get().dehumanize(start).replace(minute=0, second=0)
        except:
            pass 

    between_lower, between_upper = hours.split('-')

    new_end = None
    try: 
        new_end = arrow.get(end)
    except:
        try:
            new_end = arrow.get().dehumanize(end).replace(minute=0, second=0)
        except:
            pass 
    assert new_end, f"I do not understand --end='{end}'. Please use ISO date or ARROW Humanise syntax. ex: 'in 30 days'"
    end = new_end

    if not attendees:
        answer = inquirer.text(
            message="enter attendees",
            long_instruction='Semicolon `;` or new line `\\n` separated list. ',
            instruction=' (Use `esc` followd by `enter` to complete the question.)',
            multiline=True,
            default=''
        ).execute()
        attendees = answer.replace('\n', ";")

    new_attendees = []
    for addr in attendees.split(";"):
        addr = addr.strip()
        if not addr: continue
        if '<' in addr and '>' in addr:
            addr = addr.split('<')[1].split('>')[0]
            new_attendees.append(addr)
        else:
           new_attendees.append(addr)
    attendees = list(set(new_attendees))

    alternative_tz = alternative_tz.split(',') if alternative_tz else []
    for itz, tz in enumerate(alternative_tz): 
        if tz not in zoneinfo.available_timezones():
            alternative_tz[itz] = inquirer.fuzzy(
                message="Cannot find the TZ. Please select timezone:",
                choices=sorted(zoneinfo.available_timezones()),
                default=tz,
            ).execute()

    print ('Looking up agendas for', '; '.join(attendees))

    if sys.platform == 'darwin':
        outlook = OutlookMac()
    elif sys.platform.startswith('win'):
        outlook = OutlookWin()
    else:
        raise NotImplementedError(f'{sys.platform} not supported')

    interval = 15 if lenght <=30 else 30 
    freebusy = {}
    for index, _ in enumerate(attendees):
        while True: # for retry
            attendee = attendees[index]
            try:
                freebusy[attendee] = outlook.get_freebusy(attendee, start, end, interval=interval)
                break
            except Exception as exc:
                print(exc)
                answer = inquirer.text(
                    message=f"Cannot find {attendee} in outlook. Please correct email or delete to skip",
                    long_instruction='',
                    default=attendee
                ).execute()
                if not answer: 
                    freebusy[attendee] = None
                    break
                attendees[index] = answer

    freebusy = dict([item for item in freebusy.items() if item[1] ]) #filter empty 
    current_time_start = None
    current_slot_status = 'not started'
    current_busy_attendees = []

    busy_statuses = ['oof', 'busy'] if tentative else ['oof', 'busy', 'tentative']

    if full:
        # show all slots
        choices = []
        last_date = ''
        for time in list(freebusy.values())[0]:
            line  = time.format('    HH:mm ')
            if time.format('HHmm') > between_upper or time.format('HHmm') < between_lower: continue
            if time.format('ddd') in ("Sat", "Sun"): continue
            if time.format("dddd DD MMMM") != last_date:
                last_date = time.format("dddd DD MMMM")
                choices.append(Separator(last_date))
            num_free = 0
            for att in freebusy:
                status = freebusy[att][time][2]
                char = {
                    'oof': '▓',
                    'busy':'█',
                    'tentative': '░',
                    'free': ' '
                }.get(status, '?')
                if status not in busy_statuses:
                    num_free +=1
                line +=  char * 5 + '│'
            line +=   f'{num_free:2d}/{len(freebusy)} ' + num_free * '█'
            if (num_free + 0.5) / len(freebusy) * 100 > rate:
                choices.append({"name":line, "value": (time, time.shift(minutes=lenght))})
            else:
                choices.append(Separator(line))

        res = prompt(questions=[
            {
                "type": "list",
                "message": "Select a Timeslot to create a meeting (busy='█', free=' '):\n            " + ' '.join([key[:5] for key in freebusy]),
                "choices": choices,
                "long_instruction": f"You can choose slots with {rate}% rate. Use '--rate 50' parameter to select slots with partial availability."
            },
        ])

    else:
        # show only slots wher verybody is available
        slots = {}
        for time in list(freebusy.values())[0]:
            num_free = 0
            busy_attendees = []
            for att in freebusy:
                free = True
                if freebusy[att][time][2] in busy_statuses: free = False
                if time.format('HHmm') > between_upper or time.format('HHmm') < between_lower: free = False
                if time.format('ddd') in ("Sat", "Sun"): free = False
                if free:
                    num_free += 1
                else:
                    busy_attendees.append(att)
            busy_attendees = sorted(busy_attendees)

            # do not show domains if they are all the same
            domains = [email.split('@')[1] for email in busy_attendees]
            if len(set(domains)) == 1:
                busy_attendees = [email.split('@')[0] for email in busy_attendees]

            # check if this time ends a started slot
            # fixme: a change in busy_attendees should also trigger a slot end, but in this case the slot can be splitted in 2. 
            if (num_free + 0.5) / len(freebusy) * 100 <= rate \
               or (current_slot_status == "started" and current_busy_attendees != busy_attendees): 
                if current_slot_status == "started" and current_time_start.shift(minutes=lenght - 1) < time:
                    slot_name = '%20s' % current_time_start.format("dddd DD MMMM " + fmt) + ' - '  + time.format(fmt) + " " + str(LOCAL_TIMEZONE)
                    all_tz_free = True
                    for tz in alternative_tz:
                        time_start_tz = current_time_start.to('local').to(tz)
                        time_end_tz = time.to(tz)
                        if time_end_tz.format('HHmm') > between_upper or time_start_tz.format('HHmm') < between_lower:
                            all_tz_free = False
                        slot_name += ' / '  +  time_start_tz.format(fmt) + ' - '  + time_end_tz.to(tz).format(fmt) + " " + str(tz)
                    if current_busy_attendees:
                        slot_name += ' (N/A: ' + ','.join(current_busy_attendees) + ')'
                    if all_tz_free:
                        slots[slot_name] = (current_time_start, time)
                current_time_start = None
                current_slot_status = "not started"
            if (num_free + 0.5) / len(freebusy) * 100 >  rate:
                if not current_time_start:
                    current_time_start = time
                    current_busy_attendees = busy_attendees
                current_slot_status = "started"

        res = prompt(questions=[
            {
                "type": "list",
                "message": "Select a Timeslot to create a meeting:",
                "choices": [{"name": "Cancel (CTRL+C)", "value": None}] + [Separator()]
                        + [{"name":slots_name, "value": slots_data} for slots_name, slots_data in slots.items()],
                "default": "default",
            },
        ])

    if res[0] is None: 
        return

    start_time, end_time = res[0]

    outlook.create_event(
        subject='Placeholder',
        content='Hello,\n',
        attendees=list(freebusy.keys()),
        start_time=start_time,
        end_time=end_time,
    )

    # if inquirer.select(
    #         message="event created. are you happy with this event?",
    #         choices=['yes, thank you', 'no, delete event'],
    #     ).execute() == 'no, delete event':
    #     event.delete()



if __name__ == '__main__':
    main()