

def Main(operation, args):
    if operation == "CallNep5Contract":
        if len(args) != 4:
            return False
        from_acct = args[0]
        to_acct = args[1]
        value = args[2]
        hash = args[3]
        return CallNep5Contract(from_acct, to_acct, value, hash)


def CallNep5Contract(from_acct, to_acct, value, contractHash):
    if not TransferNEP5(from_acct, to_acct, contractHash, value):
        raise Exception()
    return True


def TransferNEP5(from_acct, to_acct, assetID, amount):
    args = [from_acct, to_acct, amount]
    contract = assetID.ToDelegate()
    if not contract("transfer", args):
        return False
    return True
