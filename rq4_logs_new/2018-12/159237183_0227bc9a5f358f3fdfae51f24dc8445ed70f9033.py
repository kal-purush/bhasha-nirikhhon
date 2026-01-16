import arcade as ARC


SCREEN_WIDTH = 500
SCREEN_HEIGHT = 800

CORE_WIDTH = 40
TICK_WIDTH = CORE_WIDTH
TICK_HEIGHT = 20


class Task:
    def __init__(self, core, start, length, index):
        self.core = core
        self.start = start
        self.length = length
        self.index = index

    def bottom_left(self):
        HEIGHT = self.length * TICK_HEIGHT
        START = CORE_WIDTH + (self.start-1) * TICK_HEIGHT
        x = self.core * CORE_WIDTH
        y = SCREEN_HEIGHT - (START + HEIGHT)
        return x,y

    def upper_right(self):
        WIDTH = TICK_WIDTH
        START = CORE_WIDTH + (self.start-1) * TICK_HEIGHT
        x = self.core * CORE_WIDTH + WIDTH
        y = SCREEN_HEIGHT - START
        return x,y



class Link:
    def __init__(self, src, dst, weight):
        self.src = src
        self.dst = dst
        self.weight = weight


def draw_field(cores, max_tick):
    LAST_COORD = max_tick * TICK_HEIGHT + CORE_WIDTH
    for core in range(0, cores + 2):
        ARC.draw_line(
                core * CORE_WIDTH,
                SCREEN_HEIGHT,
                core * CORE_WIDTH,
                SCREEN_HEIGHT - LAST_COORD,
                ARC.color.BLACK, 1)
        if core > 0 and core <= cores:
            ARC.draw_text(str(core),
                    core * CORE_WIDTH + CORE_WIDTH/3,
                    SCREEN_HEIGHT - CORE_WIDTH/4,
                    ARC.color.BLACK, 12, anchor_x="left", anchor_y="top")
    for tick in range(0, max_tick + 1):
        ARC.draw_line(
                0,
                SCREEN_HEIGHT - tick * TICK_HEIGHT - CORE_WIDTH,
                CORE_WIDTH * (cores + 1),
                SCREEN_HEIGHT - tick * TICK_HEIGHT - CORE_WIDTH,
                ARC.color.BLACK, 1)
        if tick > 0:
            ARC.draw_text(str(tick),
                    CORE_WIDTH / 4,
                    SCREEN_HEIGHT - (tick-1) * TICK_HEIGHT - CORE_WIDTH/1,
                    ARC.color.BLACK, 12, anchor_x="left", anchor_y="top")


# 1-based indexing
def draw_task(task):
    x1,y1 = task.bottom_left()
    x2,y2 = task.upper_right()
    w,h = x2-x1, y2-y1
    ARC.draw_xywh_rectangle_filled(x1, y1, w, h, ARC.color.HELIOTROPE_GRAY)
    ARC.draw_xywh_rectangle_outline(x1, y1, w, h, ARC.color.BLACK, 4)
    ARC.draw_text(str(task.index),
            x1 + CORE_WIDTH/2,
            y1 + (task.length / 2) * TICK_HEIGHT,
            ARC.color.BLACK, 12, anchor_x="left", anchor_y="bottom")


def draw_line(line, tasks):
    x1,y1 = tasks[line.src].bottom_left()
    x1 = x1 + TICK_WIDTH/2
    x2,y2 = tasks[line.dst].upper_right()
    x2 = x2 - TICK_WIDTH/2
    ARC.draw_line(x1,y1,x2,y2, ARC.color.RED, 3)
    ARC.draw_text(str(line.weight), (x1+x2)/2, (y1+y2)/2, ARC.color.BLUE, 14)


def draw(tasks, connections):
    draw_field(4, 20)
    for task in tasks:
        draw_task(task)
    for line in connections:
        draw_line(line, tasks)
    # ARC.draw_point(20, y, ARC.color.BLUE, 10)


def main():
    ARC.open_window(SCREEN_WIDTH, SCREEN_HEIGHT, "Static Planning")
    ARC.set_background_color(ARC.color.WHITE)
    ARC.start_render()

    tasks = []
    tasks.append(Task(3,1, 4, 0))
    tasks.append(Task(1,6, 3, 1))
    connections = []
    connections.append(Link(0,1, 4))
    draw(tasks, connections)

    ARC.finish_render()
    ARC.run()


if __name__ == "__main__":
    main()