import dis
import logging

server_logger = logging.getLogger('server_logger')


class ServerVerifier(type):
    def __init__(self, class_name, base_classes, class_dict):
        methods = []
        attrs = []

        for function in class_dict:
            try:
                ret = dis.get_instructions(class_dict[function])
            except TypeError:
                pass
            else:
                for i in ret:
                    print(i)
                    if i.opname == 'LOAD_GLOBAL' and i.opname not in methods:
                        methods.append(i.argval)
                    elif i.opname == 'LOAD_ATTR' and i.opname not in attrs:
                        attrs.append(i.argval)
        print(methods)
        print(attrs)

        if 'connect' in methods:
            server_logger.critical('метод connect ошибочно используется при работе сервера')

        if not 'sock' in attrs:
            server_logger.critical('неверная инициализация сокета. необходимы sock')

        super().__init__(class_name, base_classes, class_dict)
