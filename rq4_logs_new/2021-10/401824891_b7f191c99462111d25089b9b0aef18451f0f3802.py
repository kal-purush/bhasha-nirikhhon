import json
import os
import time
from pathlib import Path

from card0_manager import Card0
from logging_settings import set_logger
from monitors_manager import Monitors
from xrandrmanager import XrandrManager
class Launch:



    @staticmethod
    def monitoring_activity():
        while True:
            Card0.check_cable_conditions_until_it_change(CABLES_CONDITIONS_FILE_PATH)
            xrandr = XrandrManager()

            if len(xrandr.monitors_data) == 1:
                execute_command = 'xrandr --auto'
                logger.info(execute_command)

            # при пустом файлу st_size возвращает 1, а не 0...
            if not Path(MONITORS_LAYOUTS_FILE_PATH).is_file() or Path(MONITORS_LAYOUTS_FILE_PATH).stat().st_size < 2:
                position_manually()

            else:
                with open(MONITORS_LAYOUTS_FILE_PATH, 'r') as file:
                    previously_saved_mons_pos = json.load(file)

                location_of_monitors = xrandr.get_needed_monitors_layout_config_automatically(xrandr.monitors_data,
                                                                                              previously_saved_mons_pos)

                if location_of_monitors:
                    execute_command = xrandr.create_string_for_execute(xrandr.monitors_data, location_of_monitors)
                    logger.debug(f'Позиционируем мониторы согласно сохраненным ранее настройкам: {location_of_monitors}')

                    logger.info(execute_command)

                else:
                    logger.info(
                        'Config with previous monitor layouts doesnt contains current connected cables. Connecting monitors with sorting by monitors size. If you want to set monitors by yout self, use command: "moncotrol -s"')
                    monitors_sorted_by_size = Monitors.connect_monitors_automatically(xrandr.monitors_data)

                    with open(MONITORS_LAYOUTS_FILE_PATH, 'w') as file:
                        json.dump(monitors_sorted_by_size, file, indent=4)

                    execute_command = xrandr.create_string_for_execute(xrandr.monitors_data, monitors_sorted_by_size)

                    logger.info(execute_command)

            time.sleep(3)

            # os.system(execute_command)


    @staticmethod
    def position_manually():
        monitors = Monitors()
        xrandr = XrandrManager()

        my_monitors_position = monitors.set_monitors_position_manually()

        # TODO: сделать проверку на существование и пустоту файла

        if not Path(MONITORS_LAYOUTS_FILE_PATH).is_file() or Path(MONITORS_LAYOUTS_FILE_PATH).stat().st_size < 2:

            with open(MONITORS_LAYOUTS_FILE_PATH, 'w') as file:
                json.dump([my_monitors_position], file)

            execute_command = xrandr.create_string_for_execute(xrandr.monitors_data, my_monitors_position)

        else:
            with open(MONITORS_LAYOUTS_FILE_PATH, 'r') as file:
                previously_saved_mons_pos = json.load(file)

            new_collection_of_monitors_positions = xrandr.create_new_collections_of_mon_positions(my_monitors_position,
                                                                                                  previously_saved_mons_pos)

            with open(MONITORS_LAYOUTS_FILE_PATH, 'w') as file:
                json.dump(new_collection_of_monitors_positions, file, indent=4)

            execute_command = xrandr.create_string_for_execute(xrandr.monitors_data,
                                                               new_collection_of_monitors_positions[0])

        logger.info('Settings was saved successfully')
        logger.info(execute_command)


    def wrong_input(mode, settings):
        line_break = '\n'
        logger.critical(f'\nСкрипт не имеет настройки: {mode}\n\n'
                        f'\nЗапустите скрит с одной из следующих настроек:{line_break}{line_break}\n'
                        f'{f"{line_break}".join(settings)}')


    @staticmethod
    def choose_one_of_saved_positions_of_monitors():
        if not Path(MONITORS_LAYOUTS_FILE_PATH).is_file() or Path(MONITORS_LAYOUTS_FILE_PATH).stat().st_size == 0:
            logger.info('Config file empty or doesnt exist')

        else:
            with open(MONITORS_LAYOUTS_FILE_PATH, 'r') as file:
                previously_saved_mons_pos = json.load(file)

            # TODO: возможно, стоит сделать отдельную функцию для принта конфигов
        if not previously_saved_mons_pos:
            logger.info('You did not save any monitors layouts yet')
        logger.info('Next strings will show monitors_positions in format: {config_id: [cable_1, cable_2, etc]}\n')

        saved_cable_position_with_id = XrandrManager.create_layout_id_layout_name_dict(previously_saved_mons_pos)

        # TODO: здесь должен быть logger
        for id, monitors_names in saved_cable_position_with_id.items():
            print(f'\n{id}: {monitors_names}')

        print('\n')

        logger.info('Write number of config, that u need rn and press Enter button')
        input_id = int(input())

        xrandr = XrandrManager()
        execute_command = xrandr.create_string_for_execute(xrandr.monitors_data, saved_cable_position_with_id[input_id])
        logger.info(execute_command)


    @staticmethod
    def show_saved_monitors_positions():
        if not Path(MONITORS_LAYOUTS_FILE_PATH).is_file() or Path(MONITORS_LAYOUTS_FILE_PATH).stat().st_size == 0:
            logger.info('Config file empty or doesnt exist')

        else:
            with open(MONITORS_LAYOUTS_FILE_PATH, 'r') as file:
                saved_positions = json.load(file)

            saved_positions_with_position_id = XrandrManager.create_layout_id_layout_name_dict(saved_positions)

            logger.info(
                'Next strings will show monitors_positions in format: {monitors_position_id: [cabel_1, cabel_2, etc]}')

            for id, monitors_names in saved_positions_with_position_id.items():
                print(f'\n{id}: {monitors_names}')
            print('\n')


    @staticmethod
    def delete_config():
        if not Path(MONITORS_LAYOUTS_FILE_PATH).is_file() or Path(MONITORS_LAYOUTS_FILE_PATH).stat().st_size == 0:
            logger.info('Config file empty or doesnt exist: saved layouts doesnt exits')

        else:
            with open(MONITORS_LAYOUTS_FILE_PATH, 'r') as file:
                previously_saved_mons_pos = json.load(file)

        new_configs = Monitors.delete_saved_config(previously_saved_mons_pos)

        with open(MONITORS_LAYOUTS_FILE_PATH, 'w') as file:
            json.dump(new_configs, file)

        logger.info(
            f'This configs was deleted successfully: {[config for config in previously_saved_mons_pos if config not in new_configs]}')

logger = set_logger()
MONITORS_LAYOUTS_FILE_PATH = 'my_monitor_layout.json'
CABLES_CONDITIONS_FILE_PATH = 'cables_conditions_from_card0.json'