import sys
from PyQt6.QtWidgets import (QApplication, QMainWindow, QPushButton, 
                            QVBoxLayout, QHBoxLayout, QWidget, QFileDialog,
                            QLabel, QSlider, QListWidget, QFrame, QToolTip,
                            QTreeWidget, QTreeWidgetItem, QHeaderView, QStyledItemDelegate)
from PyQt6.QtCore import Qt, QTimer, QSize, QPoint, QMimeData, QRect
from PyQt6.QtGui import (QPixmap, QPainter, QColor, QPen, QImage, QLinearGradient, 
                        QBrush, QDragEnterEvent, QDropEvent, QFont, QIcon)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MacAmp")
        self.setGeometry(100, 100, 800, 600)
        self.setStyleSheet("""
            QMainWindow {
                background-color: #111111;
            }
            QLabel {
                color: #ffffff;
            }
        """)
        
        # Créer le widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Créer le layout vertical principal
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(3, 3, 3, 3)
        
        # Créer un layout horizontal pour les contrôles de lecture
        controls_layout = QHBoxLayout()
        controls_layout.setSpacing(10)
        controls_layout.setContentsMargins(0, 15, 0, 0)
        
        # Création des boutons
        button_size = 45
        self.prev_button = QPushButton("⏮")
        self.play_button = QPushButton("▶")
        self.stop_button = QPushButton("⏹")
        self.next_button = QPushButton("⏭")
        self.shuffle_button = QPushButton()
        self.repeat_button = QPushButton()
        
        # Charger les icônes
        shuffle_icon = QPixmap("shuffle-solid.svg")
        # Créer une nouvelle image avec fond transparent
        icon_size = QSize(24, 24)
        new_icon = QPixmap(icon_size)
        new_icon.fill(Qt.GlobalColor.transparent)
        
        # Dessiner l'icône en blanc
        painter = QPainter(new_icon)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Redimensionner l'icône source
        scaled_icon = shuffle_icon.scaled(icon_size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
        
        # Appliquer la couleur blanche
        painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceOver)
        painter.fillRect(new_icon.rect(), QColor(0, 0, 0, 0))  # Transparent
        painter.setPen(Qt.GlobalColor.white)
        painter.drawPixmap((icon_size.width() - scaled_icon.width()) // 2,
                         (icon_size.height() - scaled_icon.height()) // 2,
                         scaled_icon)
        painter.end()
        
        self.shuffle_button.setIcon(QIcon(new_icon))
        self.shuffle_button.setIconSize(icon_size)
        
        repeat_icon = QPixmap("repeat-solid.svg")
        self.repeat_button.setIcon(QIcon(repeat_icon))
        self.repeat_button.setIconSize(QSize(24, 24))
        
        buttons = [
            (self.prev_button, 18),
            (self.play_button, 22),
            (self.stop_button, 18),
            (self.next_button, 18),
            (self.shuffle_button, 18),
            (self.repeat_button, 18)
        ]
        
        # Ajouter un spacer pour pousser les boutons vers le centre
        controls_layout.addStretch()
        
        for button, font_size in buttons:
            button.setFixedSize(button_size, button_size)
            button.setStyleSheet(f"""
                QPushButton {{
                    font-size: {font_size}px;
                    background-color: #2d2d2d;
                    border-radius: {button_size//2}px;
                    padding: 0px;
                    margin: 0px;
                    color: #ffffff;
                }}
                QPushButton:hover {{
                    background-color: #3d3d3d;
                }}
            """)
            controls_layout.addWidget(button)
            
        # Ajouter un autre spacer pour centrer l'ensemble
        controls_layout.addStretch()
        
        # Ajouter les contrôles au layout principal
        main_layout.addLayout(controls_layout)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())