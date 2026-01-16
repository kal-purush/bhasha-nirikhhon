import enum

class UIEnum(enum.Enum):
    Main_menu = 0
    Inform = 1
    Settings = 2
    Pause = 3
    Game = 4

state = UIEnum.Main_menu.value