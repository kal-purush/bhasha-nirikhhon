#!/usr/bin/env python


import argparse
import db_dev as db

def auto(args):
    db.weekly_update()

def reset(args):
    db.full_db()

def deact(args):
    db.deactiv_update()

def clear_db(args):
    db.db_reset()


def main():
    db.init()
    parser = argparse.ArgumentParser(
        description=' **AUTOCROSS** - a NPPES Database and File Handler Script')

    sp = parser.add_subparsers()

    sp_clear = sp.add_parser('clear', help='Clear database.')
    sp_clear.set_defaults(func=clear_db)

    sp_deact = sp.add_parser('deact', help='Apply monthly deactivation file')
    sp_deact.set_defaults(func = deact)

    sp_reset = sp.add_parser('reset', help='Resets AutoCross')
    sp_reset.set_defaults(func=reset)
    sp_auto = sp.add_parser('auto', help='Runs AutoCross Automatically')
    sp_auto.set_defaults(func=auto)
    #Setting args to the arguments to Run
    args = parser.parse_args()
    args.func(args)



if __name__ == '__main__':
    main()


