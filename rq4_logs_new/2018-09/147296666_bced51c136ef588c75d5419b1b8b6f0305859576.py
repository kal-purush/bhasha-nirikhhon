from boa.interop.Ontology.Native import Invoke
from boa.builtins import ToScriptHash, state
from boa.interop.System.Runtime import Notify


# ONT Big endian Script Hash: 0x0100000000000000000000000000000000000000
OntContract = ToScriptHash("AFmseVrdL9f9oyCzZefL9tG6UbvhUMqNMV")
# ONG Big endian Script Hash: 0x0200000000000000000000000000000000000000
OngContract = ToScriptHash("AFmseVrdL9f9oyCzZefL9tG6UbvhfRZMHJ")


def Main(operation, args):
    if operation == "transferOntOng":
        return TransferOntOng()


def TransferOntOng(from_acct, to_acct, ont, ong):
    param = state(from_acct, to_acct, ont)
    res = Invoke(0, OntContract, "transfer", [param])
    if res != b'\x01':
        Notify("transferONT succeed")
        raise Exception("tansfer ont error.")
    param = state(from_acct, to_acct, ong)
    ret = Invoke(0, OngContract, "transfer", [param])
    if ret != b'\x01':
        Notify("transferONG succeed")
        raise Exception("tansfer ong error.")
    return True