from grid import Grid


# AbstractTabs is a class that represents navigation elements made of tabs
# As stated in the name, this class is abstract, i.e. should not be instantiated,
# an instance of one of the subclasses should be used instead.
# It conceptually differs from Grid in the way selected cells are handled
# AbstractClass distinguishes two properties of a cell (here: a tab): opened and active
# While the active cell is deactivated each time the grid itself is,
# an opened cell stays open even if the grid is deactivated
# This enables the user to see what was selected previously without the need to reselect it now
# Unlike a Grid, AbstractTabs calculates its offset depending
# on self.opened_cell instead of self.active_cell
# It also sets self.active_cell to self.opened_cell (instead of (0, 0))
# when the grid is newly selected
# AbstractTabs behaves in such a way that a cell may not be active without being opened,
# though it is technically possible to make it that way
# Attributes:
# - self.opened_cell - (int, int), coordinates of the opened grid cell
class AbstractTabs(Grid):
    # Initialises the grid
    def __init__(self, width: int, height: int):
        super().__init__(width, height)
        self.opened_cell = (0, 0)

    # Sets the old opened cell to closed and sets the new one to open
    def set_opened_cell(self, new: tuple[int, int]):
        self.get_cell(*self.opened_cell).set_opened(False)
        self.opened_cell = new
        self.get_cell(*self.opened_cell).set_opened(True)

    # Getter for self.opened_cell
    def get_opened_cell(self) -> tuple[int, int]:
        return self.opened_cell

    # Is called when the grid itself was (de)activated by its parent widget
    # Resets self.active_cell to self.opened_cell when being activated
    # When being deactivated, it also deactivates self.active_cell
    def set_active(self, new_active: bool):
        if self.active == new_active:
            return

        super().set_active(new_active)
        if new_active:
            self.set_active_cell(self.opened_cell)
            return

        self.get_cell(*self.active_cell).set_active(False)

    # Calculates and updates self.x_offset and self.y_offset depending on self.opened_cell
    def update_offset(self):
        opened_x, opened_y = self.opened_cell

        # If the offset is too low (the opened cell is below the visible part of the grid)
        if self.pref_heights[opened_y] > self.y_offset + self.height:
            self.y_offset = self.pref_heights[opened_y] - self.height

        # If the offset is too high (the opened cell is above the visible part of the grid)
        if (self.pref_heights[opened_y - 1] if opened_y != 0 else 0) < self.y_offset:
            self.y_offset = (self.pref_heights[opened_y - 1] if opened_y != 0 else 0)

        # If the offset is too left (the opened cell is to the right of the visible part of the grid)
        if self.pref_widths[opened_x] > self.x_offset + self.width:
            self.x_offset = self.pref_widths[opened_x] - self.width

        # If the offset is too right (the opened cell is to the left of the visible part of the grid)
        if (self.pref_widths[opened_x - 1] if opened_x != 0 else 0) < self.x_offset:
            self.x_offset = (self.pref_widths[opened_x - 1] if opened_x != 0 else 0)