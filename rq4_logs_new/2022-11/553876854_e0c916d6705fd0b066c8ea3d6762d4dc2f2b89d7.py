from rich.console import Console
from rich.prompt import Prompt
from rich.console import Group
from rich.panel import Panel

opstr = "[bold][yellow]Open[/yellow][blue]Py[/blue][/bold]"
console = Console()
def WelcomeScreen():
    console.print(Panel(f"Welcome to {opstr}!", title_align='center'))
    WelcomeBox = Panel("This App will help you to set up OpenPy. If this\nis your first time using OpenPy, please read the instructions [red]CAREFULLY![/red]")
    console.print(WelcomeBox)
    print()
    Prompt.ask('Press Enter to Continue')

def EULAScreen():
    EULAgroup = Group(
        Panel('Please read the EULA (End User License Agreement) and DLA (Developer License Agreement) before accepting them.\n\n', title='EULA')
    )
    console.print(EULAgroup)