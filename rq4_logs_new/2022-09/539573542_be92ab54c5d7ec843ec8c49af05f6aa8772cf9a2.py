from PyQt5.QtWidgets import QCalendarWidget
from PyQt5.QtGui import QPainter, QBrush, QColor
from PyQt5.QtCore import QRect, QDate
from DBConnection import DBConnection


class CalendarWidget(QCalendarWidget):
    def __init__(self, parent=None):
        super(CalendarWidget, self).__init__(parent)
        self.db = DBConnection()
        self.dates_with_events = self.db.get_dates_with_events()

    def paintCell(self, painter: QPainter, rect: QRect, date: QDate):
        super().paintCell(painter, rect, date)
        if date.toString("yyyy-MM-dd") in self.dates_with_events:
            painter.fillRect(rect, QColor(0, 0, 150, 50))
        