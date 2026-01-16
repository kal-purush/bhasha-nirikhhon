import msgpackrpc

def main():
    client = msgpackrpc.Client(msgpackrpc.Address("localhost", 3000))
    result = client.call("sum", 1, 2)

    print(result)


from mprpc import RPCClient

def main2():
    client = RPCClient("localhost", 3000)
    ans = client.call("sum", 1, 2)
    print(ans)


