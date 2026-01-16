from concurrent.futures import wait
from locale import resetlocale
from bs4 import BeautifulSoup
from rich.console import Console
from rich.theme import Theme
import sys
import re

ARGS = sys.argv[1:]

console = Console(highlight=False, theme=Theme({
    'red': '#f87474', 
    'green': '#6edf58',
    'blue': '#1363df',
}))
def error(err, err_type, line='std'):
    console.print(f"""[red]error[/red] in [blue]<{line}>[/blue]:
    returns that [red]error[/red] by [blue]{err_type} error [/blue] (
        "[green]{err}[/green]"
    )[red];[/red]""")
    quit()


try:
    file_name = ARGS[0]
except IndexError:
    error('file name not found in specified arguments.', 'argument_specify')

if 'file_name' in globals():
    if file_name.split('.')[-1] not in (file_extensions:=['htm', 'html']):
        error(f'file extension not supportable for usage.\n\t use one of file extension in {file_extensions}', 'file_extension')
    
    try:
        with open(file_name) as f:
            content = f.read()
            f.close()
        
        if not content.strip():
            quit()
        else:
            FUNCTIONS = {
                'pow': {
                    'op': '**'
                },
                'add': {
                    'op': '+'
                },
                'sub': {
                    'op': '-'
                },
                'divm': {
                    'op': '/'
                },
                'mod': {
                    'op': '%'
                },
                'mult': {
                    'op': '*'
                }
            }
            VARIABLES = {}
            soup = BeautifulSoup(content, 'html.parser')

            saves = soup.find_all("save")
            for save in saves:
                for var in save.attrs:
                    VARIABLES[var] = {}
                    VARIABLES[var]["value"] = save.attrs[var]
                    try:
                        VARIABLES[var]["type"]  = re.findall(r"^\<class \'(.*)\'\>$", str(eval(f"type({save.attrs[var]})")))[0]
                    except SyntaxError:
                        del VARIABLES[var]
                        error(f'cannot to set {var}\'s type.', 'variable_type')
            
            for var in VARIABLES:
                try:
                    soup.find(var).replaceWith(VARIABLES[var]["value"])
                except AttributeError:
                    pass
            
            for func in FUNCTIONS:
                
                func_list = soup.find_all(func)
                if len(func_list) > 1:
                    for _func in func_list:
                        arg_list = []
                        res = FUNCTIONS[func]["op"].join(arg_list)

                        print(eval(res)) if res else ''
                else:
                    try:
                        _func = func_list[0]
                        res = FUNCTIONS[func]["op"].join([_func.attrs[i] for i in _func.attrs])
                        
                        try:
                            for var in VARIABLES:
                                res = res.replace(f"@{var}", VARIABLES[var]["value"])
                            soup.find(_func.name, attrs=_func.attrs).replaceWith(str(eval(res)))
                        except SyntaxError:
                            error(f'cannot operate that operation', "operation")
                        except TypeError: 
                            error('cannot operate that operation', "operation")
                    except IndexError:
                        pass
            
            with open(file_name, "w+") as f:
                f.write(soup.prettify())
                f.close()
            

    except FileNotFoundError:
        error(f'file with name {file_name} not found.', 'file_not_found')