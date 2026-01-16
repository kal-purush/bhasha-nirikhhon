
def simulate(size, shard_size):
    avg = len(size) / float(shard_size)
    out = []
    last = 0.0

    while last < len(size):
        out.append(size[int(last):int(last + avg)])
        last += avg

    return out

if __name__ == '__main__':
    print(simulate(range(100), 3))
    print(simulate(range(100), 10))