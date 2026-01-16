from ms import app
from .hello import hello


app.cli.add_command(hello)