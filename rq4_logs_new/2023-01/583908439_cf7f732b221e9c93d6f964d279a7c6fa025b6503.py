import flet as ft
from flet import *
from flet import colors, icons


class AppTodo(UserControl):

    def build(self):
        self.name = TextField(label="Add Task",color=colors.BLACK,hint_text="Add task")
        self.listtodo = Column(auto_scroll=True)
        return Column(
            alignment="center",
            controls=[
                Row([
                    self.name,
                    FloatingActionButton(
                        icon=icons.ADD,
                        bgcolor=colors.ORANGE_600,
                        on_click=self.addnew

                    )

                ],
                alignment = ft.MainAxisAlignment.CENTER,
                ),
                self.listtodo

            ]

        )

    def addnew(self, e):
        self.listtodo.controls.append(
            Container(
                content=Text(self.name.value,
                             color=colors.WHITE
                             ),
                bgcolor=colors.CYAN,
                margin=10,
                padding=10,
                border_radius=120
            )
        )
        self.update()


def main(page: Page):
    page.bgcolor = ft.colors.WHITE
    page.title = "New Year To-Do app"
    page.update()
    todo = AppTodo()
    page.add(todo)

ft.app(target=main)