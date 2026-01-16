from dataclasses import dataclass, field
from random import random
from typing import Iterable, Mapping, TypeAlias


@dataclass(frozen=True)
class OS:
    chance_to_infect: int | float

    def __post_init__(self) -> None:
        if not 0 <= self.chance_to_infect <= 1:
            raise ValueError("`chance_to_infect` must be between 0 and 1")


@dataclass(unsafe_hash=True)
class Computer:
    name: str
    os: OS
    is_infected: bool = field(default=False, kw_only=True)


def can_get_infected(computer: Computer) -> bool:
    return (
        computer.os.chance_to_infect != 0
        and computer.os.chance_to_infect >= random()
    )


Network: TypeAlias = Mapping[Computer, Iterable[Computer]]


def infect_in(network: Network) -> None:
    for computer_to_infect in tuple(
        computer_to_infect
        for root_computer, computers_to_infect in network.items()
        if root_computer.is_infected
        for computer_to_infect in computers_to_infect
        if can_get_infected(computer_to_infect)
    ):
        computer_to_infect.is_infected = True


def is_state_static_in(computers: Iterable[Computer]) -> bool:
    computers = tuple(computers)

    return (
        all(computer.is_infected for computer in computers)
        or all(not computer.is_infected for computer in computers)
    )


def show_state_in(computers: Iterable[Computer]) -> None:
    print("state:")

    for computer in computers:
        print("\t{}: {}".format(
            computer.name,
            'infected' if computer.is_infected else 'not infected',
        ))


def run(network: Network) -> None:
    network = {_: tuple(computers) for _, computers in network.items()}
    computers_in_network = frozenset((
        *network.keys(),
        *sum(network.values(), tuple()),
    ))

    show_state_in(computers_in_network)

    while not is_state_static_in(computers_in_network):
        infect_in(network)
        show_state_in(computers_in_network)


def main():
    a = Computer('a', OS(0.5), is_infected=True)
    b = Computer('b', OS(0.5))
    c = Computer('c', OS(0.75))
    d = Computer('d', OS(0.25))
    e = Computer('e', OS(0.5))
    f = Computer('f', OS(0.75))

    run({
        a: [b, e],
        b: [c, e],
        c: [a, e],
        d: [b, c],
        e: [d, f],
        f: [b, a],
    })


if __name__ == "__main__":
    main()


"""
output:
state:
    a: infected
    b: not infected
    c: not infected
    d: not infected
    e: not infected
    f: not infected
state:
    a: infected
    b: infected
    c: not infected
    d: not infected
    e: not infected
    f: not infected
state:
    a: infected
    b: infected
    c: not infected
    d: not infected
    e: infected
    f: not infected
state:
    a: infected
    b: infected
    c: infected
    d: not infected
    e: infected
    f: infected
state:
    a: infected
    b: infected
    c: infected
    d: not infected
    e: infected
    f: infected
state:
    a: infected
    b: infected
    c: infected
    d: not infected
    e: infected
    f: infected
state:
    a: infected
    b: infected
    c: infected
    d: infected
    e: infected
    f: infected
"""