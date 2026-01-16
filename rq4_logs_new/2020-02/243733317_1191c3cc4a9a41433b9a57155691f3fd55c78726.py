import pickle
import os
import yaml
from typing import List, Iterator, Dict


class Pickler:
    """
    Описывает класс обмена переменными между облаком (Colab) и локальной машиной
    """
    def __init__(self, exclution_vars: List[str] = [],
                 var_prefix: str = 'gv',
                 store_path: str = "env/", config_path='pickler/config.yaml'):

        # Переменные, которые нужно исключить их снхронизации
        self.exclution_vars: List[str] = exclution_vars
        # Префикс имени переменной которая будет синхронизирована
        self.var_prefix: str = var_prefix
        # Путь хранения переменных
        self.store_path: str = store_path
        # Путь к файлу конфигурации
        self.config_path: str = config_path

        # зпускает конфигурирование Pickler
        self._configure()

    def _configure(self)-> None:
        """
        Загрузка конфигурации
        :return: None
        """
        with open(self.config_path, 'r') as conf:
            self._config = yaml.safe_load(conf)
        # Переменные для исключения из сохранения
        self.exclution_vars = list(set(self.exclution_vars + self._config['exclution_vars']))

    def _do_name_filter(self, context: object) -> Iterator[str]:
        """
        Фильтр переменных для сохранения
        :param context: object
            Область переменных
        :return: Iterator[str]
            Итератор имен переменных
        """
        # Проходим по глобальным переменным и выбираем переменные с префиксом
        variable_names = list(context.keys()).copy()
        for var in variable_names:
            if var not in self.exclution_vars:  # Если переменная не в списке исклбчения
                if var.startswith(self.var_prefix):  # Если переменная содержит префикс
                    yield var

    def _do_save_type(self, variable_value) -> str:
        return 'pickle'

    def _do_load_type(self, variable_name) -> str:
        return 'pickle'

    def save(self, context: object, silent: bool = True) -> None:
        """
        Сохраняет переменные
        :param context: object
            Область переменных
        :param silent: bool
            Оповещение о результате выполнения
        :return: None
        """
        vars_for_save = list(self._do_name_filter(context))  # Выберем переменные для сохранения
        for variable_name in vars_for_save:
            variable_value = context[variable_name]  # Извлекаем значение переменной
            save_type: str = self._do_save_type(variable_value)  # Определяем тип переменной
            self._save(save_type, variable_name, variable_value)  # Сохраняем переменную
        if not silent:
            print(f"Saved {len(vars_for_save)} variables.")

    def _save(self, save_type: str, variable_name: str, variable_value: object) -> None:
        """
            Сохраняет переменные
        :param save_type: str
            Тип переменной для сохранения
        :param variable_name: str
            Имя переменной
        :param variable_value:
            Значение переменной
        :return:
        """
        new_variable_name = f"{variable_name}_{save_type}"  # Новое имя для переменной
        if save_type == "pickle":
            self._save_pickle(variable_value, new_variable_name)

    def load(self, silent: bool = True) -> Dict[str, object]:
        """
        Восстановление переменных
        :param silent: bool
            Оповещение о выполнении операции
        :return: Dict[str, object]
            Словарь с восстановленными переменными
        """
        loaded_vars = {}
        for var_path in os.listdir(self.store_path):
            load_type = self._do_load_type(var_path)
            variable_name = var_path.split('.')[0]
            new_variable_name = variable_name.split("_")[0]  # Новое имя для переменной
            loaded_vars[new_variable_name] = self._load(load_type, new_variable_name, var_path)
        if not silent:
            print(f"Loaded {len(loaded_vars)} variables.")
        return loaded_vars

    def _load(self, load_type: str, variable_name: str, variable_path: str) -> object:
        """
        Восстанавливает переменную из файла
        :param load_type: str
            Тип сохраненной переменной(извлекается из имени файла)
        :param variable_name: str
             Имя переменной
        :param variable_path: str
            Путь к переменной
        :return: object
            Объект загруженной переменной
        """
        if load_type == "pickle":
            return self._load_pickle(variable_name, variable_path)

    def get_variables(self, context: object, silent: bool = False) -> List[str]:
        """
        Возвращает список переменных которые будут сериализованы
        :param context: object
            Область переменных
        :param silent: bool
            Оповещение о результате выполнения
        :return:
        """
        list_vars = list(self._do_name_filter(context))
        if not silent:
            print(f"Variables Count: {len(list_vars)}")
        return list_vars

    def add_exclution_vars(self, vars: List[str], silent: bool = True) -> None:
        """
        Добавить переменные в список исключения
        :param vars: List[str]
            Список переменных
        :param silent: bool
            Оповещение о результате выполнения
        :return:
        """
        self.exclution_vars = list(set(self.exclution_vars + vars))
        if not silent:
            print(f"Exclution vars: {len(self.exclution_vars)}")

    def remove_from_exclution(self, vars: List[str], silent: bool = True) -> None:
        """
        Удалить переменные из списка исключения
        :param vars: List[str]
            Список переменных
        :param silent: bool
            Оповещение о результате выполнения
        :return:
        """
        self.exclution_vars = list(set(self.exclution_vars) - set(vars))
        if not silent:
            print(f"Exclution vars: {len(self.exclution_vars)}")

    def show_exclution_vars(self) -> List[str]:
        """
        Показывает список переменных для исключения
        :return: List[str]
            Список переменных
        """
        return self.exclution_vars

    def _save_pickle(self, var: object, name: str) -> None:
        """
        Сохраняет переменную в pickle файл
        :param var: object
            Переменная
        :param name: str
            Имя переменной
        :return: None
        """
        if not os.path.exists(self.store_path):
            os.makedirs(self.store_path)
        pickle.dump(var, open(f"{self.store_path}{name}{'.pkl'}", "wb"))

    def _load_pickle(self, variable_name: str, variable_path) -> object:
        """
        Восстанавливает переменную из файла pickle
        :param variable_name: str
            Имя переменной
        :param variable_path: str
            Путь к переменной
        :return: object
            Объект переменной
        """
        return pickle.load(open(f"{self.store_path}{variable_path}", "rb"))