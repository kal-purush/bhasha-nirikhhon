import os
import typer
import importlib
import inspect
from prettytable import PrettyTable

# initiate cli application library
app = typer.Typer()

# hard-coded modules directory //TODO move it to a yaml config
modules_directory = "modules"

# list all the modules present and print their methods and path
@app.command()
def list():
    
    # print("listing loaded modules")
    # fetch and load modules
    module_list = loadModules()
    # initiate pretty table
    module_prettytable = PrettyTable()
    module_prettytable.field_names = ["MODULE(S)", "METHOD(S)", "FILE"]
    
    for module in module_list:
        # print out methods available
        # predicate is meant to pick out just the method name from the list of tuple
        method_list = [x.__name__ for x in module.__dict__.values() if inspect.isfunction(x)]
        # fill in text with dash if no methods available
        if len(method_list) <= 0:
            method_list = '-'
        # add module name and methods into table
        module_prettytable.add_row([module.__name__.replace(modules_directory + ".", ""), method_list, module.__file__])
    
    print(module_prettytable)

# call a specific method from a module
@app.command()
def sudo(module_name: str, module_method: str):
    
    # fetch and load modules
    module_list = loadModules()
    
    for module in module_list:
        # find for module thru module name
        if module.__name__ == modules_directory + "." + module_name:
            # check if method exists within module
            if module_method in [x.__name__ for x in module.__dict__.values() if inspect.isfunction(x)]:
                try:
                    # try calling method
                    method = getattr(module, module_method)
                    method()
                except Exception as e:
                    print("[x] " + module.__name__ + " method error: " + e)
            else:
                print("[x] no such method exists in " + module.__name__)
                
            return
                
    print ("[x] no such module exists")
    
    
def loadModules():
    
    print("[*] loading modules")
    
    # module list variable
    modules_list = []
    
    # iteration for module loaded count
    iteration = 0
    
    # invalidates any cache storage of module
    # importlib.invalidate_caches()
    
    # check for modules in module directory and try to load them
    for fileName in os.listdir(modules_directory):
        
        file = os.path.join(modules_directory, fileName)
        # debug text, prints out file path
        # print("file found:" + file)

        # checks if file is a python script
        if os.path.isfile(file) and fileName.endswith('.py'):
            
            try:
                # removes file extension
                fileName = fileName.replace('.py', '')
                # import module thru relative path
                module = importlib.import_module(modules_directory + "." + fileName)
                # add imported module into list
                modules_list.append(module)
                # debug text, list out methods within module
                # print(dir(module))
                print("[+] " + module.__file__)
                iteration += 1
                
            except ImportError:
                print("[x] error trying to import module:" + file)
                
    print ("[#] total loaded " + str(iteration) + " module(s)")
    
    return modules_list

if __name__ == '__main__':
    app()
         