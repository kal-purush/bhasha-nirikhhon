# cmd에서 pip3 install PyQt5 명령어를 이용하여 PyQt5를 설치해야 한다
#박스 레이아웃 사용 (https://wikidocs.net/21945 참조)

import sys
from PyQt5.QtWidgets import *
from PyQt5 import QtGui, QtCore
from PyQt5.QtGui import QFont


class MyApp(QWidget):

    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Squirtle') #프로그램 이름
        font = QFont('나눔고딕', 15) #폰트 설정
        self.setFont(font)
        self.resize(1080, 720) #창 사이즈

        ##버튼 구현
        ConfigBtn = QPushButton('', self) #ConfigBtn 버튼 구현
        ConfigBtn.setIcon(QtGui.QIcon('..\..\Assets\icon\config.svg')) #아이콘 구현 (상대경로)
        ConfigBtn.setIconSize(QtCore.QSize(50,50)) #아이콘 크기
        ConfigBtn.setFlat(True) #버튼 테두리 없애기
        ConfigBtn.clicked.connect(self.ConfigBtnClicked) #버튼 클릭 했을 때 ConfigBtn_clicked 함수 호출

        ##ComboBox 구현
        HCombo = QComboBox(self) #시
        MCombo = QComboBox(self) #분
        HCombo.addItem('1')
        MCombo.addItem('1')

        ##Box 레이아웃
        #수평
        hboxUp = QHBoxLayout() #수평 box 생성
        hboxUp.addWidget(ConfigBtn) #ConfigBtn 버튼 생성
        hboxUp.addStretch(1) #비율이 1인 빈 공간 생성
        
        hboxMid = QHBoxLayout() #ComboBox 레이아웃
        hboxMid.setContentsMargins(450, 0, 450, 0)
        hboxUp.addStretch(1)
        hboxMid.addWidget(HCombo)
        hboxMid.addWidget(MCombo)
        hboxUp.addStretch(1)
        
        #수직
        vbox = QVBoxLayout() #수직 box 생성
        vbox.addLayout(hboxUp)
        vbox.addStretch(1)
        vbox.addLayout(hboxMid)
        vbox.addStretch(1)

        self.setLayout(vbox) #수직 box를 메인 레이아웃으로 설정

        ##창 출력
        self.center() #창을 화면 중앙으로
        self.show() #창 출력

    def ConfigBtnClicked(self): #Config 버튼 클릭
        QMessageBox.about(self, 'MessageBox', '클릭!') #QMessageBox 메세지 박스

    #def hComboSet(hbox):


    
    
    def center(self): #창의 화면을 중앙으로
        qr = self.frameGeometry() #창의 위치와 크기 정보를 가져온다
        cp = QDesktopWidget().availableGeometry().center() #현재 모니터의 위치를 파악한다
        qr.moveCenter(cp) #창을 cp로 이동한다
        self.move(qr.topLeft())


if __name__ == '__main__':
   app = QApplication(sys.argv)
   ex = MyApp()
   sys.exit(app.exec_())