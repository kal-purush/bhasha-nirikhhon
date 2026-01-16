class ActionType:
    click = "click"


class DetectType:
    color = "color"


class Action:
    def __init__(self, action_type, x, y, r, g, b, notification, detect):
        self.action_type = action_type
        self.x = x
        self.y = y
        self.r = r
        self.g = g
        self.b = b
        self.notification = notification
        self.detect = detect

    def getActionString(self):
        return f'{self.action_type} {self.x} {self.y} {self.r} {self.g} {self.b} {self.notification} {self.detect}'


class Settings:
    replay_loops = 0
    log_comments = None
    log_actions = None
    log_debug = None
    click_delay_min = None
    click_delay_max = None
    notification_delay = None
    notification_loops = None