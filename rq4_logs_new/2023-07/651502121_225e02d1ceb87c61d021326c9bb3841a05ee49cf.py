from typing import Generator
from logging import getLogger
from subprocess import run
from itertools import chain

logger = getLogger("get_generator.py")


def generate_pid_errors(log_file: str, output_file: str = "pid_errors.log") -> None:
    run(
        f"grep -oP '\[PRODUCT_ERROR\]:\s\w*' {log_file} | awk -F\" \" '{{ print $2 }}' 1> {output_file}",
        shell=True,
    )
    return output_file


def get_failed_products_generator() -> Generator[str, None, None]:
    pid_errors = generate_pid_errors("logs.log")
    lines = []
    with open(pid_errors, "r", encoding="utf-8") as file:
        for line in file:
            lines.append(f"https://amazon.com.br/dp/{line.strip()}")
    return (l for l in lines)