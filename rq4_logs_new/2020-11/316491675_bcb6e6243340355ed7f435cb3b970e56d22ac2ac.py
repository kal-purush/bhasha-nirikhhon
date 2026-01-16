import sys
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton
from PyQt5.QtWidgets import QLabel, QLineEdit
import pygame
from PyQt5.QtGui import QPixmap

pygame.init()


class Music(QLabel, object):  # тут музыкааа

    def __init__(self):
        super().__init__()
        self.setGeometry(1500, 700, 1500, 700)
        self.setWindowTitle('Piano')

        pixmap = QPixmap('pi')
        self.image_unpressed = QLabel(self)
        self.image_unpressed.move(0, 0)
        self.image_unpressed.setPixmap(pixmap)
        pixmap = QPixmap('pi1.png')

        self.image_pressed_do = QLabel(self)
        self.image_pressed_do.move(0, 0)
        self.image_pressed_do.setPixmap(pixmap)
        self.image_pressed_do.hide()

        pixmap = QPixmap('pi2.png')
        self.image_pressed_do_sharp = QLabel(self)
        self.image_pressed_do_sharp.move(0, 0)
        self.image_pressed_do_sharp.setPixmap(pixmap)
        self.image_pressed_do_sharp.hide()

        pixmap = QPixmap('pi3.png')
        self.image_pressed_re = QLabel(self)
        self.image_pressed_re.move(0, 0)
        self.image_pressed_re.setPixmap(pixmap)
        self.image_pressed_re.hide()

        pixmap = QPixmap('pi4.png')
        self.image_pressed_re_sharp = QLabel(self)
        self.image_pressed_re_sharp.move(0, 0)
        self.image_pressed_re_sharp.setPixmap(pixmap)
        self.image_pressed_re_sharp.hide()

        pixmap = QPixmap('pi5.png')
        self.image_pressed_mi = QLabel(self)
        self.image_pressed_mi.move(0, 0)
        self.image_pressed_mi.setPixmap(pixmap)
        self.image_pressed_mi.hide()

        pixmap = QPixmap('pi6.png')
        self.image_pressed_fa = QLabel(self)
        self.image_pressed_fa.move(0, 0)
        self.image_pressed_fa.setPixmap(pixmap)
        self.image_pressed_fa.hide()

        pixmap = QPixmap('pi7.png')
        self.image_pressed_fa_sharp = QLabel(self)
        self.image_pressed_fa_sharp.move(0, 0)
        self.image_pressed_fa_sharp.setPixmap(pixmap)
        self.image_pressed_fa_sharp.hide()

        pixmap = QPixmap('pi8.png')
        self.image_pressed_sol = QLabel(self)
        self.image_pressed_sol.move(0, 0)
        self.image_pressed_sol.setPixmap(pixmap)
        self.image_pressed_sol.hide()

        pixmap = QPixmap('pi9.png')
        self.image_pressed_sol_sharp = QLabel(self)
        self.image_pressed_sol_sharp.move(0, 0)
        self.image_pressed_sol_sharp.setPixmap(pixmap)
        self.image_pressed_sol_sharp.hide()

        pixmap = QPixmap('pi10.png')
        self.image_pressed_la = QLabel(self)
        self.image_pressed_la.move(0, 0)
        self.image_pressed_la.setPixmap(pixmap)
        self.image_pressed_la.hide()

        pixmap = QPixmap('pi11.png')
        self.image_pressed_la_sharp = QLabel(self)
        self.image_pressed_la_sharp.move(0, 0)
        self.image_pressed_la_sharp.setPixmap(pixmap)
        self.image_pressed_la_sharp.hide()

        pixmap = QPixmap('pi12.png')
        self.image_pressed_si = QLabel(self)
        self.image_pressed_si.move(0, 0)
        self.image_pressed_si.setPixmap(pixmap)
        self.image_pressed_si.hide()

        pixmap = QPixmap('pi13.png')
        self.image_pressed_do2 = QLabel(self)
        self.image_pressed_do2.move(0, 0)
        self.image_pressed_do2.setPixmap(pixmap)
        self.image_pressed_do2.hide()

        pixmap = QPixmap('pi14.png')
        self.image_pressed_do_sharp2 = QLabel(self)
        self.image_pressed_do_sharp2.move(0, 0)
        self.image_pressed_do_sharp2.setPixmap(pixmap)
        self.image_pressed_do_sharp2.hide()

        pixmap = QPixmap('pi15.png')
        self.image_pressed_re2 = QLabel(self)
        self.image_pressed_re2.move(0, 0)
        self.image_pressed_re2.setPixmap(pixmap)
        self.image_pressed_re2.hide()

        pixmap = QPixmap('pi16.png')
        self.image_pressed_re_sharp2 = QLabel(self)
        self.image_pressed_re_sharp2.move(0, 0)
        self.image_pressed_re_sharp2.setPixmap(pixmap)
        self.image_pressed_re_sharp2.hide()

        pixmap = QPixmap('pi17.png')
        self.image_pressed_mi2 = QLabel(self)
        self.image_pressed_mi2.move(0, 0)
        self.image_pressed_mi2.setPixmap(pixmap)
        self.image_pressed_mi2.hide()

        pixmap = QPixmap('pi18.png')
        self.image_pressed_fa2 = QLabel(self)
        self.image_pressed_fa2.move(0, 0)
        self.image_pressed_fa2.setPixmap(pixmap)
        self.image_pressed_fa2.hide()

        pixmap = QPixmap('pi19.png')
        self.image_pressed_fa_sharp2 = QLabel(self)
        self.image_pressed_fa_sharp2.move(0, 0)
        self.image_pressed_fa_sharp2.setPixmap(pixmap)
        self.image_pressed_fa_sharp2.hide()

        pixmap = QPixmap('pi20.png')
        self.image_pressed_sol2 = QLabel(self)
        self.image_pressed_sol2.move(0, 0)
        self.image_pressed_sol2.setPixmap(pixmap)
        self.image_pressed_sol2.hide()

        pixmap = QPixmap('pi21.png')
        self.image_pressed_sol_sharp2 = QLabel(self)
        self.image_pressed_sol_sharp2.move(0, 0)
        self.image_pressed_sol_sharp2.setPixmap(pixmap)
        self.image_pressed_sol_sharp2.hide()

        pixmap = QPixmap('pi22.png')
        self.image_pressed_la2 = QLabel(self)
        self.image_pressed_la2.move(0, 0)
        self.image_pressed_la2.setPixmap(pixmap)
        self.image_pressed_la2.hide()

        pixmap = QPixmap('pi23.png')
        self.image_pressed_la_sharp2 = QLabel(self)
        self.image_pressed_la_sharp2.move(0, 0)
        self.image_pressed_la_sharp2.setPixmap(pixmap)
        self.image_pressed_la_sharp2.hide()

        pixmap = QPixmap('pi14.png')
        self.image_pressed_si2 = QLabel(self)
        self.image_pressed_si2.move(0, 0)
        self.image_pressed_si2.setPixmap(pixmap)
        self.image_pressed_si2.hide()

        pixmap = QPixmap('pi15.png')
        self.image_pressed_do_lower = QLabel(self)
        self.image_pressed_do_lower.move(0, 0)
        self.image_pressed_do_lower.setPixmap(pixmap)
        self.image_pressed_do_lower.hide()

        pixmap = QPixmap('pi17.png')
        self.image_pressed_re_lower = QLabel(self)
        self.image_pressed_re_lower.move(0, 0)
        self.image_pressed_re_lower.setPixmap(pixmap)
        self.image_pressed_re_lower.hide()

        pixmap = QPixmap('pi19.png')
        self.image_pressed_mi_lower = QLabel(self)
        self.image_pressed_mi_lower.move(0, 0)
        self.image_pressed_mi_lower.setPixmap(pixmap)
        self.image_pressed_mi_lower.hide()

        pixmap = QPixmap('pi21.png')
        self.image_pressed_fa_lower = QLabel(self)
        self.image_pressed_fa_lower.move(0, 0)
        self.image_pressed_fa_lower.setPixmap(pixmap)
        self.image_pressed_fa_lower.hide()

        pixmap = QPixmap('pi23.png')
        self.image_pressed_sol_lower = QLabel(self)
        self.image_pressed_sol_lower.move(0, 0)
        self.image_pressed_sol_lower.setPixmap(pixmap)
        self.image_pressed_sol_lower.hide()

        pixmap = QPixmap('pi25.png')
        self.image_pressed_la_lower = QLabel(self)
        self.image_pressed_la_lower.move(0, 0)
        self.image_pressed_la_lower.setPixmap(pixmap)
        self.image_pressed_la_lower.hide()

        pixmap = QPixmap('pi27.png')
        self.image_pressed_si_lower = QLabel(self)
        self.image_pressed_si_lower.move(0, 0)
        self.image_pressed_si_lower.setPixmap(pixmap)
        self.image_pressed_si_lower.hide()

        pixmap = QPixmap('token_2.png')
        self.image_pressed = QLabel(self)
        self.image_pressed.move(0, 0)
        self.image_pressed.setPixmap(pixmap)
        self.image_pressed.hide()

    def play_do(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_do
        self.image_pressed.show()

        pygame.mixer.music.load('do.wav')
        pygame.mixer.music.play()

    def play_do_sharp(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_do_sharp
        self.image_pressed.show()

        pygame.mixer.music.load('dosharp.wav')
        pygame.mixer.music.play()

    def play_re(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_re
        self.image_pressed.show()

        pygame.mixer.music.load('re.wav')
        pygame.mixer.music.play()

    def play_re_sharp(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_re_sharp
        self.image_pressed.show()

        pygame.mixer.music.load('resharp.wav')
        pygame.mixer.music.play()

    def play_mi(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_mi
        self.image_pressed.show()

        pygame.mixer.music.load('mi.wav')
        pygame.mixer.music.play()

    def play_fa(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_fa
        self.image_pressed.show()

        pygame.mixer.music.load('fa.wav')
        pygame.mixer.music.play()

    def play_fa_sharp(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_fa_sharp
        self.image_pressed.show()

        pygame.mixer.music.load('fa_sharp.wav')
        pygame.mixer.music.play()

    def play_sol(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_sol
        self.image_pressed.show()

        pygame.mixer.music.load('sol.wav')
        pygame.mixer.music.set_volume(130)
        pygame.mixer.music.play()

    def play_sol_sharp(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_sol_sharp
        self.image_pressed.show()

        pygame.mixer.music.load('sol_sharp.wav')
        pygame.mixer.music.set_volume(130)
        pygame.mixer.music.play()

    def play_la(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_la
        self.image_pressed.show()

        pygame.mixer.music.load('la.wav')
        pygame.mixer.music.set_volume(500)
        pygame.mixer.music.play()

    def play_la_sharp(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_la_sharp
        self.image_pressed.show()

        pygame.mixer.music.load('lasharp.wav')
        pygame.mixer.music.set_volume(130)
        pygame.mixer.music.play()

    def play_si(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_si
        self.image_pressed.show()

        pygame.mixer.music.load('si.wav')
        pygame.mixer.music.set_volume(300)
        pygame.mixer.music.play()

    def play_do2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_do2
        self.image_pressed.show()

        pygame.mixer.music.load('d02.wav')
        pygame.mixer.music.play()

    def play_do_sharp2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_do_sharp2
        self.image_pressed.show()

        pygame.mixer.music.load('dosharp2.wav')
        pygame.mixer.music.play()
        while True:
            for i in pygame.event.get():
                if i.type == pygame.QUIT:
                    exit()

    def play_re2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_re2
        self.image_pressed.show()

        pygame.mixer.music.load('re2.wav')
        pygame.mixer.music.play()

    def play_re_sharp2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_re_sharp2
        self.image_pressed.show()

        pygame.mixer.music.load('resharp2.wav')
        pygame.mixer.music.play()

    def play_mi2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_mi2
        self.image_pressed.show()

        pygame.mixer.music.load('mi2.wav')
        pygame.mixer.music.play()

    def play_fa2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_fa2
        self.image_pressed.show()

        pygame.mixer.music.load('fa2.wav')
        pygame.mixer.music.play()

    def play_fa_sharp2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_fa_sharp2
        self.image_pressed.show()

        pygame.mixer.music.load('fasharp2.wav')
        pygame.mixer.music.play()

    def play_sol2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_sol2
        self.image_pressed.show()

        pygame.mixer.music.load('sol2.wav')
        pygame.mixer.music.play()

    def play_sol_sharp2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_sol_sharp2
        self.image_pressed.show()

        pygame.mixer.music.load('solsharp2.wav')
        pygame.mixer.music.play()

    def play_la2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_la2
        self.image_pressed.show()

        pygame.mixer.music.load('la2.wav')
        pygame.mixer.music.play()

    def play_la_sharp2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_la_sharp2
        self.image_pressed.show()

        pygame.mixer.music.load('lasharp2.wav')
        pygame.mixer.music.play()

    def play_si2(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_si2
        self.image_pressed.show()

        pygame.mixer.music.load('si2.wav')
        pygame.mixer.music.play()

    def play_do_lower(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_do_lower
        self.image_pressed.show()

        pygame.mixer.music.load('dolower.wav')
        pygame.mixer.music.play()

    def play_re_lower(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_re_lower
        self.image_pressed.show()

        pygame.mixer.music.load('relower.wav')
        pygame.mixer.music.play()

    def play_mi_lower(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_mi_lower
        self.image_pressed.show()

        pygame.mixer.music.load('milower.wav')
        pygame.mixer.music.play()

    def play_fa_lower(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_fa_lower
        self.image_pressed.show()

        pygame.mixer.music.load('falower.wav')
        pygame.mixer.music.play()

    def play_sol_lower(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_sol_lower
        self.image_pressed.show()

        pygame.mixer.music.load('sollower.wav')
        pygame.mixer.music.play()

    def play_la_lower(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_la_lower
        self.image_pressed.show()

        pygame.mixer.music.load('lalower.wav')
        pygame.mixer.music.play()

    def play_si_lower(self):
        self.image_unpressed.hide()
        self.image_pressed = self.image_pressed_si_lower
        self.image_pressed.show()

        pygame.mixer.music.load('silower.wav')
        pygame.mixer.music.play()

    def release(self):  # туть всё стопорится
        self.image_unpressed.show()
        self.image_pressed.hide()

    def keyPressEvent(self, event):
        # 1-ая октава
        if event.key() == Qt.Key_A:
            self.play_do()
        elif event.key() == Qt.Key_G:
            self.play_sol()
        elif event.key() == Qt.Key_S:
            self.play_re()
        elif event.key() == Qt.Key_D:
            self.play_mi()
        elif event.key() == Qt.Key_F:
            self.play_fa()
        elif event.key() == Qt.Key_H:
            self.play_la()
        elif event.key() == Qt.Key_J:
            self.play_si()
        # 2-я октава
        elif event.key() == Qt.Key_Z:
            self.play_do2()
        elif event.key() == Qt.Key_X:
            self.play_re2()
        elif event.key() == Qt.Key_C:
            self.play_mi2()
        elif event.key() == Qt.Key_V:
            self.play_fa2()
        elif event.key() == Qt.Key_B:
            self.play_sol2()
        elif event.key() == Qt.Key_N:
            self.play_la2()
        elif event.key() == Qt.Key_M:
            self.play_si2()
        # 3-я октава
        elif event.key() == Qt.Key_Q:
            self.play_do_lower()
        elif event.key() == Qt.Key_W:
            self.play_re_lower()
        elif event.key() == Qt.Key_E:
            self.play_mi_lower()
        elif event.key() == Qt.Key_R:
            self.play_fa_lower()
        elif event.key() == Qt.Key_T:
            self.play_sol_lower()
        elif event.key() == Qt.Key_Y:
            self.play_la_lower()
        elif event.key() == Qt.Key_U:
            self.play_si_lower()
        # полутона
        elif event.key() == Qt.Key_1:
            self.play_do_sharp()
        elif event.key() == Qt.Key_2:
            self.play_re_sharp()
        elif event.key() == Qt.Key_3:
            self.play_fa_sharp()
        elif event.key() == Qt.Key_4:
            self.play_sol_sharp()
        elif event.key() == Qt.Key_5:
            self.play_la_sharp()
        elif event.key() == Qt.Key_6:
            self.play_do_sharp2()
        elif event.key() == Qt.Key_7:
            self.play_re_sharp2()
        elif event.key() == Qt.Key_8:
            self.play_fa_sharp2()
        elif event.key() == Qt.Key_9:
            self.play_sol_sharp2()
        elif event.key() == Qt.Key_0:
            self.play_la_sharp2()

    def keyReleaseEvent(self, event) -> None:
        if event.key():
            self.release()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Music()
    ex.show()
    sys.exit(app.exec())