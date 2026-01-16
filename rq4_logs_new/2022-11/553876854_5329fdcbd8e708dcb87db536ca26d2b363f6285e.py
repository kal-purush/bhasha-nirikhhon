from rich.console import Console
from rich.prompt import Prompt
from rich.console import Group
from rich.panel import Panel
import framework.console as con
import os
import json


opstr = "[bold][yellow]Open[/yellow][blue]Py[/blue][/bold]"
console = Console()
def WelcomeScreen():
    con.clear()
    console.print(Panel(f"Welcome to {opstr}!", title_align='center'))
    WelcomeBox = Panel("This App will help you to set up OpenPy. If this\nis your first time using OpenPy, please read the instructions [red]CAREFULLY![/red]",title='Welcome')
    console.print(WelcomeBox)
    Prompt.ask('Press Enter to Continue')

def EULAScreen():
    con.clear()
    EULAgroup = Group(
        Panel('Please read the EULA (End User License Agreement) and DLA (Developer License Agreement) before accepting them.\n\n', title='EULA'),
    )
    console.print(EULAgroup)
    console.print('If you do not want to agree to the EULA and DLA, please close OpenPy Imediately!')
    Prompt.ask('Press Enter to Agree to the EULA and DLA')

def UsernameScreen():
    con.clear()
    UsernameGroup = Group(
        Panel('The User Setup can help OpenPy (and applications written for it) to personalize your experience.', title='Username')
    )
    console.print(UsernameGroup)
    UserName = Prompt.ask('Username', default='OpenPy User', show_default=False)
    return UserName

def GithubAPIKeyScreen(user: str):
    con.clear()
    GithubGroup = Group(
        Panel(f'Okay {user}, for this next stage, you will need a Github API Key.\nThis is used for the get-app command, which will require to access\nthe GitHub API.', title='Github API Key'),
        Panel('If you do not have a GitHub API Key, you can just press enter to skip this stage and do it later.', title='Note')
    )
    console.print(GithubGroup)
    apiKEY = Prompt.ask('Github API Key', show_default=False, default='')
    if apiKEY == "":
        return "not set"
    else:
        return apiKEY

def ColorScreen():
    con.clear()
    ThemeGroup = Group(
        Panel('To set a Theme, use the changeTheme command in the OpenPy Command Line.\nTo install themes, drag your Themes JSON file to the following folder:\n\n%AppData%\\OpenPy\\themes', title="Theme"),
    )
    console.print(ThemeGroup)
    Prompt.ask('Press Enter to Continue')

def ThankYouScreen(user: str):
    con.clear()
    console = Console()
    ThankYouGroup = Group(
        Panel(f'That was it {user}. Thank you for using OpenPy!\nI (Janlu/The Creator) hope you have a wonderful time\nusing OpenPy.')
    )
    console.print(ThankYouGroup)