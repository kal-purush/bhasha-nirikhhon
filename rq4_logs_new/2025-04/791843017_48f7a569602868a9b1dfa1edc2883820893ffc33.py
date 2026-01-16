"""Tab model/views which are based on a list at the side of the widget."""

from typing import Any, Callable, Generic, TypeVar

import RATapi
from PyQt6 import QtCore, QtGui, QtWidgets
from RATapi.utils.enums import BackgroundActions, LayerModels

from rascal2.config import path_for
from rascal2.widgets.delegates import ProjectFieldDelegate

T = TypeVar("T")


class ClassListItemModel(QtCore.QAbstractListModel, Generic[T]):
    """Item model for a project ClassList field.

    Parameters
    ----------
    classlist : ClassList
        The initial classlist to represent in this model.
    parent : QtWidgets.QWidget
        The parent widget for the model.

    """

    def __init__(self, classlist: RATapi.ClassList[T], parent: QtWidgets.QWidget):
        super().__init__(parent)
        self.parent = parent

        self.classlist = classlist
        self.item_type = classlist._class_handle
        self.edit_mode = False

    def rowCount(self, parent=None) -> int:
        return len(self.classlist)

    def data(self, index, role=QtCore.Qt.ItemDataRole.DisplayRole) -> str:
        if role == QtCore.Qt.ItemDataRole.DisplayRole:
            row = index.row()
            return self.classlist[row].name

    def get_item(self, row: int) -> T:
        """Get an item from the ClassList.

        Parameters
        ----------
        row : int
            The index of the ClassList to get.

        Returns
        -------
        T
            The relevant item from the classlist.

        """
        return self.classlist[row]

    def set_data(self, row: int, param: str, value: Any):
        """Set data for an item in the ClassList.

        Parameters
        ----------
        row : int
            The index of the ClassList to get.
        param : str
            The parameter of the item to change.
        value : Any
            The value to set the parameter to.

        """
        setattr(self.classlist[row], param, value)
        self.endResetModel()

    def append_item(self):
        """Append an item to the ClassList."""
        self.classlist.append(self.item_type())
        self.endResetModel()

    def delete_item(self, row: int):
        """Delete an item in the ClassList.

        Parameters
        ----------
        row : int
            The row containing the item to delete.

        """
        if len(self.classlist) == 0:
            return
        self.classlist.pop(row)
        self.endResetModel()


class AbstractProjectListWidget(QtWidgets.QWidget):
    """An abstract base widget for editing items kept in a list."""

    item_type = "item"
    classlist_model = ClassListItemModel

    def __init__(self, field: str, parent):
        super().__init__(parent)
        self.field = field
        self.parent = parent
        self.project_widget = self.parent.parent
        self.edit_mode = False
        self.model = None

        layout = QtWidgets.QHBoxLayout()

        item_list = QtWidgets.QVBoxLayout()

        self.list = QtWidgets.QListView(parent)
        self.list.setSizePolicy(QtWidgets.QSizePolicy.Policy.Minimum, QtWidgets.QSizePolicy.Policy.Expanding)

        buttons = QtWidgets.QHBoxLayout()

        self.add_button = QtWidgets.QPushButton("+")
        self.add_button.setHidden(True)
        self.add_button.pressed.connect(self.append_item)
        buttons.addWidget(self.add_button)

        self.delete_button = QtWidgets.QPushButton(icon=QtGui.QIcon(path_for("delete.png")))
        self.delete_button.setHidden(True)
        self.delete_button.pressed.connect(self.delete_item)
        buttons.addWidget(self.delete_button)

        item_list.addWidget(self.list)
        item_list.addLayout(buttons)

        layout.addLayout(item_list)

        self.item_view = QtWidgets.QScrollArea(parent)
        self.item_view.setWidgetResizable(True)
        layout.addWidget(self.item_view)

        self.setLayout(layout)

    def update_model(self, classlist):
        """Update the list model to synchronise with the project field.

        Parameters
        ----------
        classlist: RATapi.ClassList
            The classlist to set in the model.

        """
        self.model = self.classlist_model(classlist, self)
        self.list.setModel(self.model)
        # this signal changes the current contrast shown in the editor to be the currently highlighted list item
        self.list.selectionModel().currentChanged.connect(lambda index, _: self.view_stack.setCurrentIndex(index.row()))
        self.update_item_view()
        self.list.selectionModel().setCurrentIndex(
            self.model.index(0, 0), self.list.selectionModel().SelectionFlag.ClearAndSelect
        )

    def update_item_view(self):
        """Update the item views to correspond with the list model."""

        self.view_stack = QtWidgets.QStackedWidget(self)

        if self.model is not None:
            # if there are no items, replace the widget with information
            if self.model.rowCount() == 0:
                self.view_stack = QtWidgets.QLabel(
                    f"No {self.item_type}s are currently defined! Edit the project to add a {self.item_type}."
                )
                self.view_stack.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

            for i in range(0, self.model.rowCount()):
                if self.edit_mode:
                    widget = self.create_editor(i)
                else:
                    widget = self.create_view(i)
                self.view_stack.addWidget(widget)

            self.item_view.setWidget(self.view_stack)

    def edit(self):
        """Update the view to be in edit mode."""
        self.add_button.setVisible(True)
        self.delete_button.setVisible(True)
        self.edit_mode = True
        self.update_item_view()

    def append_item(self):
        """Append an item to the model if the model exists."""
        if self.model is not None:
            self.model.append_item()

        new_widget_index = self.model.rowCount() - 1
        # handle if no contrasts currently exist
        if isinstance(self.view_stack, QtWidgets.QLabel):
            self.view_stack = QtWidgets.QStackedWidget(self)
            self.item_view.setWidget(self.view_stack)

        # add contrast viewer/editor to stack without resetting entire stack
        if self.edit_mode:
            self.view_stack.addWidget(self.create_editor(new_widget_index))
        else:
            self.view_stack.addWidget(self.create_view(new_widget_index))

        self.list.selectionModel().setCurrentIndex(
            self.model.index(new_widget_index, 0), self.list.selectionModel().SelectionFlag.ClearAndSelect
        )

    def delete_item(self):
        """Delete the currently selected item."""
        if self.model is not None:
            selection_model = self.list.selectionModel()
            deleted_index = selection_model.currentIndex().row()
            self.model.delete_item(deleted_index)

            self.update_item_view()

            self.list.selectionModel().setCurrentIndex(
                self.model.index(deleted_index - 1, 0), self.list.selectionModel().SelectionFlag.ClearAndSelect
            )

    def create_view(self, i: int) -> QtWidgets.QWidget:
        """Create the view widget for a specific item.

        Parameters
        ----------
        i : int
            The index of the classlist item displayed by this widget.

        Returns
        -------
        QtWidgets.QWidget
            The widget that displays the classlist item.

        """
        raise NotImplementedError

    def create_editor(self, i: int) -> QtWidgets.QWidget:
        """Create the edit widget for a specific item.

        Parameters
        ----------
        i : int
            The index of the classlist item displayed by this widget.

        Returns
        -------
        QtWidgets.QWidget
            The widget that allows the classlist item to be edited.

        """
        raise NotImplementedError


class LayerStringListModel(QtCore.QStringListModel):
    """A string list that supports drag and drop."""

    def flags(self, index):
        # we disable ItemIsDropEnabled to disable overwriting of items via drop
        flags = super().flags(index)
        if index.isValid():
            flags &= ~QtCore.Qt.ItemFlag.ItemIsDropEnabled

        return flags

    def supportedDropActions(self):
        return QtCore.Qt.DropAction.MoveAction


class StandardLayerModelWidget(QtWidgets.QWidget):
    """Widget for standard layer contrast models."""

    def __init__(self, init_list: list[str], parent):
        super().__init__(parent)

        self.model = LayerStringListModel(init_list, self)
        self.domains = parent.model.domains
        self.layer_list = QtWidgets.QListView(parent)
        self.layer_list.setModel(self.model)
        if self.domains:
            self.layer_list.setItemDelegateForColumn(
                0, ProjectFieldDelegate(parent.project_widget, "domain_contrasts", self)
            )
        else:
            self.layer_list.setItemDelegateForColumn(0, ProjectFieldDelegate(parent.project_widget, "layers", self))
        self.layer_list.setDragEnabled(True)
        self.layer_list.setAcceptDrops(True)
        self.layer_list.setDropIndicatorShown(True)

        self.layer_list.selectionModel().setCurrentIndex(
            self.model.index(0, 0), QtCore.QItemSelectionModel.SelectionFlag.ClearAndSelect
        )

        self.add_button = QtWidgets.QPushButton("+")
        self.add_button.setToolTip("Add a layer after the currently selected layer (Shift+Enter)")
        if self.model.rowCount() == 2:
            self.add_button.setEnabled(False)
        add_shortcut = QtGui.QShortcut(QtGui.QKeySequence("Shift+Return"), self)
        self.add_button.pressed.connect(self.append_item)
        add_shortcut.activated.connect(self.append_item)

        delete_button = QtWidgets.QPushButton(icon=QtGui.QIcon(path_for("delete.png")))
        delete_button.setToolTip("Delete the currently selected layer (Del)")
        delete_shortcut = QtGui.QShortcut(QtGui.QKeySequence.StandardKey.Delete, self)
        delete_button.pressed.connect(self.delete_item)
        delete_shortcut.activated.connect(self.delete_item)

        edit_shortcut = QtGui.QShortcut(QtGui.QKeySequence("Tab"), self)
        edit_shortcut.activated.connect(lambda: self.edit_item())

        move_up_shortcut = QtGui.QShortcut(QtGui.QKeySequence("Shift+Up"), self)
        move_down_shortcut = QtGui.QShortcut(QtGui.QKeySequence("Shift+Down"), self)
        move_up_shortcut.activated.connect(lambda: self.move_item(-1))
        move_down_shortcut.activated.connect(lambda: self.move_item(1))

        buttons = QtWidgets.QHBoxLayout()
        buttons.addWidget(self.add_button)
        buttons.addWidget(delete_button)

        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(self.layer_list)
        layout.addLayout(buttons)
        layout.setContentsMargins(0, 0, 0, 0)

        self.setLayout(layout)

    def append_item(self):
        """Append an item below the currently selected item."""
        if self.model is not None:
            # do not allow items to be added in domains for over 2 items
            if self.domains and self.model.rowCount() == 2:
                return

            selection_model = self.layer_list.selectionModel()
            index = selection_model.currentIndex()
            self.model.insertRow(index.row() + 1)
            new_index = self.model.index(index.row() + 1, 0)
            selection_model.setCurrentIndex(new_index, selection_model.SelectionFlag.ClearAndSelect)
            self.layer_list.edit(new_index)

            # if 2 items have been reached by this adding, disable add button
            if self.domains and self.model.rowCount() == 2:
                self.add_button.setEnabled(False)

    def delete_item(self):
        """Delete the currently selected item."""
        if self.model is not None:
            selection_model = self.layer_list.selectionModel()
            index = selection_model.currentIndex()
            self.model.removeRow(index.row())
            self.model.dataChanged.emit(index, index)

            # re-enable add button if disabled
            self.add_button.setEnabled(True)

    def move_item(self, delta: int):
        """Change the currently selected item's index by a number of rows.

        Parameters
        ----------
        delta : int
            The change in index of the selected item.

        """
        if self.model is not None:
            selection_model = self.layer_list.selectionModel()
            index = selection_model.currentIndex()

            if index.row() + delta < 0:
                new_row = 0
            elif index.row() + delta >= self.model.rowCount():
                new_row = self.model.rowCount() - 1
            else:
                new_row = index.row() + delta

            new_index = self.model.index(new_row, 0)
            item_data = self.model.data(index)
            self.model.removeRow(index.row())
            self.model.insertRow(new_index.row())
            self.model.setData(new_index, item_data)
            selection_model.setCurrentIndex(new_index, selection_model.SelectionFlag.ClearAndSelect)

    def edit_item(self):
        """Open the currently selected item's editor if it isn't already open."""
        # if this check isn't here, Qt produces warnings into the terminal
        if self.layer_list.state() != self.layer_list.State.EditingState:
            self.layer_list.edit(self.layer_list.selectionModel().currentIndex())


class ContrastModel(ClassListItemModel):
    """ClassList item model for contrast data with or without a ratio."""

    def __init__(self, classlist, parent):
        super().__init__(classlist, parent)
        self.domains = classlist._class_handle == RATapi.models.ContrastWithRatio
        self.domain_ratios = {}

    def set_domains(self, domains: bool):
        """Set whether the classlist uses ContrastWithRatio.

        Parameters
        ----------
        domains : bool
            Whether the classlist should use ContrastWithRatio.

        """
        if domains != self.domains:
            self.beginResetModel()
            self.domains = domains
            if domains:
                classlist = RATapi.ClassList(
                    [
                        RATapi.models.ContrastWithRatio(
                            **dict(contrast), domain_ratio=self.domain_ratios.get(contrast.name, "")
                        )
                        for contrast in self.classlist
                    ]
                )
                # set handle manually if classlist is empty
                classlist._class_handle = RATapi.models.ContrastWithRatio
            else:
                # save domain ratios so they aren't lost if the user toggles
                # back and forth
                self.domain_ratios = {contrast.name: contrast.domain_ratio for contrast in self.classlist}
                classlist = RATapi.ClassList(
                    [
                        RATapi.models.Contrast(
                            name=contrast.name,
                            data=contrast.data,
                            background=contrast.background,
                            background_action=contrast.background_action,
                            bulk_in=contrast.bulk_in,
                            bulk_out=contrast.bulk_out,
                            scalefactor=contrast.scalefactor,
                            resolution=contrast.resolution,
                            resample=contrast.resample,
                            model=contrast.model,
                        )
                        for contrast in self.classlist
                    ]
                )
                # set handle manually if classlist is empty
                classlist._class_handle = RATapi.models.Contrast

            self.classlist = classlist
            self.item_type = classlist._class_handle
            self.parent.project_widget.update_draft_project({"contrasts": classlist})


class ContrastWidget(AbstractProjectListWidget):
    """Widget for viewing and editing Contrasts."""

    item_type = "contrast"
    classlist_model = ContrastModel

    def compose_widget(self, i: int, data_widget: Callable[[str], QtWidgets.QWidget]) -> QtWidgets.QWidget:
        """Create the base grid layouts for the widget.

        Parameters
        ----------
        i : int
            The row of the contrasts list to display in this widget.
        data_widget : Callable[[str], QtWidgets.QWidget]
            A function which takes a field name and returns the data widget for that field.

        Returns
        -------
        QtWidgets.QWidget
            The resulting widget for the item.

        """
        grid = QtWidgets.QGridLayout()
        grid.addWidget(QtWidgets.QLabel("Contrast Name:"), 0, 0)
        grid.addWidget(data_widget("name"), 0, 1, 1, -1)

        grid.addWidget(QtWidgets.QLabel("Background:"), 1, 0)
        grid.addWidget(data_widget("background"), 1, 1, 1, 2)
        grid.addWidget(QtWidgets.QLabel("Background Action:"), 1, 3)
        grid.addWidget(data_widget("background_action"), 1, 4, 1, 2)

        grid.addWidget(QtWidgets.QLabel("Resolution:"), 2, 0)
        grid.addWidget(data_widget("resolution"), 2, 1)
        grid.addWidget(QtWidgets.QLabel("Scalefactor:"), 2, 2)
        grid.addWidget(data_widget("scalefactor"), 2, 3)
        grid.addWidget(QtWidgets.QLabel("Data:"), 2, 4)
        grid.addWidget(data_widget("data"), 2, 5)
        if self.model.domains:
            grid.addWidget(QtWidgets.QLabel("Domain Ratio:"), 3, 0)
            grid.addWidget(data_widget("domain_ratio"), 3, 1, 1, -1)

        grid.setVerticalSpacing(10)

        resampling_checkbox = QtWidgets.QCheckBox()
        resampling_checkbox.setChecked(self.model.get_item(i).resample)
        resampling_checkbox.checkStateChanged.connect(
            lambda s: self.model.set_data(i, "resample", (s == QtCore.Qt.CheckState.Checked))
        )

        grid.addWidget(QtWidgets.QLabel("Use resampling:"), 4, 0)
        grid.addWidget(resampling_checkbox, 4, 1)
        grid.addWidget(QtWidgets.QLabel("Bulk in:"), 5, 0)
        grid.addWidget(data_widget("bulk_in"), 5, 1, 1, -1)
        grid.addWidget(QtWidgets.QLabel("Model:"), 6, 0)
        grid.addWidget(data_widget("model"), 6, 1, 1, -1)
        grid.addWidget(QtWidgets.QLabel("Bulk out:"), 7, 0)
        grid.addWidget(data_widget("bulk_out"), 7, 1, 1, -1)

        widget = QtWidgets.QWidget(self)
        widget.setLayout(grid)

        return widget

    def create_view(self, i: int) -> QtWidgets.QWidget:
        def data_box(field: str) -> QtWidgets.QWidget:
            """Create a read only line edit box for display."""
            current_data = getattr(self.model.get_item(i), field)
            if field == "model":
                if self.project_widget.parent_model.project.model == LayerModels.StandardLayers:
                    widget = QtWidgets.QListWidget(parent=self)
                    widget.addItems(current_data)
                else:
                    widget = QtWidgets.QLineEdit(current_data[0])
                    widget.setReadOnly(True)
            else:
                widget = QtWidgets.QLineEdit(current_data)
                widget.setReadOnly(True)

            return widget

        return self.compose_widget(i, data_box)

    def create_editor(self, i: int) -> QtWidgets.QWidget:
        self.comboboxes = {}

        def data_combobox(field: str) -> QtWidgets.QWidget:
            current_data = getattr(self.model.get_item(i), field)
            match field:
                case "name":
                    widget = QtWidgets.QLineEdit(current_data)
                    widget.textChanged.connect(lambda text: self.set_name_data(i, text))
                    return widget
                case "background_action":
                    widget = QtWidgets.QComboBox()
                    for action in BackgroundActions:
                        widget.addItem(str(action), action)
                    widget.setCurrentText(current_data)
                    widget.currentTextChanged.connect(
                        lambda: self.model.set_data(i, "background_action", widget.currentData())
                    )
                    return widget
                case "model":
                    if self.project_widget.draft_project["model"] == LayerModels.StandardLayers:
                        widget = StandardLayerModelWidget(current_data, self)
                        widget.model.dataChanged.connect(
                            lambda: self.model.set_data(i, field, widget.model.stringList())
                        )
                        widget.model.rowsMoved.connect(lambda: self.model.set_data(i, field, widget.model.stringList()))
                        return widget
                    else:
                        widget = QtWidgets.QComboBox(self)
                        widget.addItem("", [])
                        for file in self.project_widget.draft_project["custom_files"]:
                            widget.addItem(file.name, [file.name])
                        if current_data:
                            widget.setCurrentText(current_data[0])
                        else:
                            widget.setCurrentText("")
                        widget.currentTextChanged.connect(lambda: self.model.set_data(i, field, widget.currentData()))
                        return widget
                # all other cases are comboboxes with data from other widget tables
                case "data" | "bulk_in" | "bulk_out":
                    project_field_name = field
                    pass
                case _:
                    project_field_name = field + "s"
                    pass

            project_field = self.project_widget.draft_project[project_field_name]
            combobox = QtWidgets.QComboBox(self)
            items = [""] + [item.name for item in project_field]
            combobox.addItems(items)
            combobox.setCurrentText(current_data)
            combobox.currentTextChanged.connect(lambda: self.model.set_data(i, field, combobox.currentText()))
            combobox.setSizePolicy(QtWidgets.QSizePolicy.Policy.MinimumExpanding, QtWidgets.QSizePolicy.Policy.Fixed)

            return combobox

        return self.compose_widget(i, data_combobox)

    def set_name_data(self, index: int, name: str):
        """Set name data, ensuring name isn't empty.

        Parameters
        ----------
        index : int
            The index of the contrast.
        name : str
            The desired name for the contrast.

        """
        if name != "":
            self.model.set_data(index, "name", name)
        else:
            self.model.set_data(index, "name", "Unnamed Contrast")

    def set_domains(self, domains: bool):
        """Set whether the model uses ContrastWithRatio.

        Parameters
        ----------
        domains : bool
            Whether the model should use ContrastWithRatio.

        """
        self.model.set_domains(domains)
        self.update_model(self.model.classlist)