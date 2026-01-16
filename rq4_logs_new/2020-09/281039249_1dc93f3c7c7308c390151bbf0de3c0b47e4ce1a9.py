from manimlib.imports import *


class C_to_F(Scene):
    def construct(self):
        self.black_box(w=1)
        text_01 = TextMobject("Input: 0, 8, 10").move_to(2 * UP)
        text_02 = TextMobject("Input: 32, 8, 10").move_to(2 * DOWN)

        self.play(
            Write(text_01),
        )
        self.wait()
        self.play(
            Write(text_02)
        )
        self.wait()

    def black_box(self, w=1):
        text_01 = TextMobject("Black Box")
        rect = SurroundingRectangle(text_01, buff=1.0)

        self.play(
            Write(text_01),
            ShowCreation(rect)
        )

        self.wait(w)