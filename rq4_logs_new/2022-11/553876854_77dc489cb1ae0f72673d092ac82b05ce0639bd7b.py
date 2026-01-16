from rich.console import Console
from rich.prompt import Prompt
from rich.console import Group
from rich.panel import Panel

opstr = "[bold][yellow]Open[/yellow][blue]Py[/blue][/bold]"
console = Console()
class screens:
    def WelcomeScreen():
        console.print(Panel(f"Welcome to {opstr}!", title_align='center'))
        WelcomeBox = Panel("This App will help you to set up OpenPy. If this\nis your first time using OpenPy, please read the instructions [red]CAREFULLY![/red]")
        console.print(WelcomeBox)
        Prompt.ask('Press Enter to Continue')

    def EULAScreen():
        EULAgroup = Group(
            Panel('Please read the EULA (End User License Agreement) and DLA (Developer License Agreement) before accepting them.\n\n', title='EULA'),
        )
        console.print(EULAgroup)
        console.print('If you do not want to agree to the EULA and DLA, please close OpenPy Imediately!')
        Prompt.ask('Press Enter to Agree to the EULA and DLA')

class Setup:
    screens.WelcomeScreen()
    screens.EULAScreen()