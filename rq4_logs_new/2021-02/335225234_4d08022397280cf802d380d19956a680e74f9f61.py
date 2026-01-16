import click
import logging
from puzzle.log import setup_logging
from puzzle.puzzle import PuzzleSolver, PuzzleInput


@click.command()
@click.option("--input", help="Filepath to file that that contains puzzle input")
@click.option("--debug", default=False, required=False)
def run(input, debug):
    click.echo(f'Running with {input} file.')
    setup_logging(logging.DEBUG if debug else logging.INFO)

    puzzle_input = PuzzleInput(puzzle_input_path=input)
    puzzle_input.load()
    puzzle_solver = PuzzleSolver(puzzle_input.data)
    puzzle_solver.run()
    puzzle_solver.get_result()


if __name__ == '__main__':
    run()