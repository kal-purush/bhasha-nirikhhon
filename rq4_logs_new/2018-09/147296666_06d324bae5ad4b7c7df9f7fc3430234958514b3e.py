"""
Lottery Game
"""
from boa.interop.System.Storage import *
from boa.interop.System.Runtime import *
from boa.interop.System.ExecutionEngine import *
from boa.interop.System.Runtime import *
from boa.interop.Ontology.Native import *
from boa.builtins import state
from boa.builtins import concat

contractAddress = bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02')
ctx = GetContext()
selfAddr = GetExecutingScriptHash()
# gameKey = "Game_"
roundKey = "Round_"
attendCntKey = "AttendCnt_"
attendeeKey = "Attendee_"
indexKey = "Index_"
# status_running = "RUNNING"
# status_running = "DONE"
ongPerTicket = 1000000000


def Main(operation, args):
    if operation == 'attend':
        account = args[0]
        ticketsCount = args[1]
        if ticketsCount != 5 and ticketsCount != 10 and ticketsCount != 20:
            return False
        return attend(account, ticketsCount)


def attend(acct, count):
    if CheckWitness(acct):
        key = concat(roundKey, count)
        status = Get(ctx, key)
        # initial a game
        if not status:
            if transONG(acct, selfAddr, ongPerTicket):
                # record current round
                Put(ctx, key, 1)
                # add attend count
                Put(ctx, concat(attendCntKey, key), 1)
                # record attendee
                Put(ctx, concat(concat(attendeeKey, key), acct), 1)
                # record attendee index
                Put(ctx, concat(concat(indexKey, key), 1), acct)
            else:
                Notify("transfer ong failed!")
                return False
        else:
            attendedCount = Get(ctx,concat(attendCntKey, key))
            # the game is still running
            if attendedCount < count:
                newCount = attendedCount + 1
                if transONG(acct, selfAddr, ongPerTicket):
                    # add attend count
                    Put(ctx, concat(attendCntKey, key), newCount)
                    # get attendee tickets
                    tickets = Get(ctx,concat(concat(attendeeKey, key), acct))
                    if not tickets:
                        # record attendee
                        Put(ctx, concat(concat(attendeeKey, key), acct), 1)
                    else:
                        Put(ctx, concat(concat(attendeeKey, key), acct), tickets + 1)

                    # record attendee index
                    Put(ctx, concat(concat(indexKey, key), newCount), acct)
                else:
                    Notify("transfer ong failed!")
                    return False

                if newCount == count:
                    endGame(count)
                else:
                    return True
    else:
        Notify("CheckWiteness failed!")
        return False

def endGame(count):
    key = concat(roundKey, count)
    attendcount = Get(ctx, concat(attendCntKey, key))
    # currentTime =


def transONG(fromacct,toacct,amount):
    """
     transfer ONT
     :param fromacct:
     :param toacct:
     :param amount:
     :return:
     """
    if CheckWitness(fromacct):

        param = makeState(fromacct, toacct, amount)
        res = Invoke(1, contractAddress, 'transfer', [param])
        Notify(res)

        if res and res == b'\x01':
            Notify('transfer succeed')
            return True
        else:
            Notify('transfer failed')

            return False

    else:
        Notify('checkWitness failed')
        return False


def makeState(fromacct,toacct,amount):
    return state(fromacct, toacct, amount)