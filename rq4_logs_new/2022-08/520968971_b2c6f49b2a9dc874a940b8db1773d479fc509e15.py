#!/usr/bin/python3
"""
contains the entry point of the command interpreter
"""
import cmd
import shlex
import os
import models
from models import BaseModel, User, Amenity, Review, City, Place, State

storage = models.storage
classes_dict = {"Amenity": Amenity, "BaseModel": BaseModel, "City": City,
                "Place": Place, "Review": Review, "State": State, "User": User}


class HBNBCommand(cmd.Cmd):
    """ a command line interpreter """
    prompt = '(hbnb) '
    keyss = classes_dict.keys()

    def do_quit(self, s):
        """ exit operation """
        return True

    def do_EOF(self, s):
        """ exit operation """
        return True

    def emptyline(self):
        """ empty line + ENTER shouldn’t execute anything """
        return False

    def do_create(self, s):
        """ Creates a new instance of BaseModel
            saves it (to the JSON file) and prints the id
        """
        if len(s) == 0:
            print('** class name missing **')
        else:
            try:
                args = s.split()
                new = eval("{}()".format(args[0]))
                new.save()
                print(new.id)
            except Exception:
                print("** class doesn't exist **")

    def do_show(self, arg):
        """
        Show command Prints the string representation of an instance based
        on
        the class name and id
        Usage: show <Class_Name> <obj_id>
        """
        args = arg.split()
        if len(args) == 0:
            print("** class name missing **")
            return
        try:
            eval(args[0])
        except Exception:
            print("** class doesn't exist **")
            return
        if len(args) == 1:
            print("** instance id missing **")
        else:
            storage.reload()
            container_obj = storage.all()
            key_id = args[0] + "." + args[1]
            if key_id in container_obj:
                value = container_obj[key_id]
                print(value)
            else:
                print("** no instance found **")

    def do_destroy(self, s):
        """ deletes an instance
            based on the class name and id
        """
        args = s.split()
        obj = []
        if len(args) == 0:
            print("** class name missing **")
            return
        try:
            eval(args[0])
        except Exception:
            print("** class doesn't exist **")
            return
        if len(args) == 1:
            print("** instance id missing **")
        else:
            storage.reload()
            obj = storage.all()
            key_id = args[0] + "." + args[1]
            if key_id in obj:
                del obj[key_id]
                storage.save()
            else:
                print("** no instance found **")

    def do_all(self, s):
        """ Prints all string representation of all instances """
        storage.reload()
        arg = s.split()
        lists = []
        cont = storage.all()
        if len(arg) == 0:
            for arg_id in cont.keys():
                obj = cont[arg_id]
                lists.append(str(obj))
            print(lists)
            return
        if len(arg) == 1:
            try:
                eval(arg[0])
                for arg_id in cont.keys():
                    if type(cont[arg_id] is eval(arg[0])):
                        obj = cont[arg_id]
                        lists.append(str(obj))
                print(lists)
            except Exception:
                print("** class doesn't exist **")
                return

    def do_update(self, _input):
        """Updates an instance based on the class name and id by adding
           or updating attribute. save the change into the JSON file
        """
        _input = shlex.split(_input)
        query_key = ''

        if len(_input) == 0:
            print("** class name missing **")
            return
        if _input[0] not in self.keyss:
            print("** class doesn't exist **")
            return
        if len(_input) == 1:
            print("** instance id missing **")
            return
        if len(_input) > 1:
            query_key = _input[0] + '.' + _input[1]
        if query_key not in models.storage.all().keys():
            print("** no instance found **")
            return
        if len(_input) == 2:
            print('** attribute name missing **')
            return
        if len(_input) == 3:
            print('** value missing **')
            return
        key_name = _input[2]
        input_value = _input[3]
        setattr(models.storage.all()[query_key], key_name, input_value)

        models.storage.all()[query_key].save()

    def default(self, inp):
        """Retrieve all instances class using: <class name>.all()"""
        count = 0
        words = inp.split(".")

        if words[0] in classes_dict and words[1] == "all()":
            self.do_all(words[0])
        elif words[0] in classes_dict and words[1] == "count()":
            if (words[0] not in classes_dict):
                print("** class doesn't exist **")
                return (False)
            else:
                for key in models.storage.all():
                    if key.startswith(words[0]):
                        count += 1
                print(count)
        elif words[0] in classes_dict and words[1].startswith('show'):
            arg = words[1].split('"')
            if len(arg) == 3:
                arg1 = words[0] + " " + arg[1]
                self.do_show(arg1)
        elif words[0] in classes_dict and words[1].startswith('destroy'):
            arg = words[1].split('"')
            if len(arg) == 3:
                arg1 = words[0] + " " + arg[1]
                self.do_destroy(arg1)
        elif words[0] in classes_dict and words[1].startswith('update'):
            start = 'update('
            end = ')'
            arg = re.findall(re.escape(start)+"(.*)"+re.escape(end), words[1])[0]
            arg = arg.replace('(', '').replace(')', '').replace(',', '')
            arg = arg.replace('"', '')
            arg1 = words[0] + " " + arg
            self.do_update(arg1)
        else:
            print("*** Unknown syntax: {}".format(inp))


if __name__ == '__main__':
    HBNBCommand().cmdloop()