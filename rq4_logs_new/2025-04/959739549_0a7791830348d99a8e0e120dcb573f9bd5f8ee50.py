from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QListWidget, QTableWidget, QTableWidgetItem, QPushButton,
    QLabel, QLineEdit
)
from PyQt6.QtCore import Qt, QTimer
import sys
import os
from ssh_config_parser import parse_ssh_config
from tunnel_manager import TunnelManager

class SSHTunnelApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SSH Tunnel Manager")
        self.resize(1920, 1080)

        self.tunnel_manager = TunnelManager()

        self.sidebar = QListWidget()
        self.sidebar.itemClicked.connect(self.load_host_ports)

        self.port_table = QTableWidget(0, 4)
        self.port_table.setHorizontalHeaderLabels(["Local Port", "Remote Port", "Actions", "Delete"])

        layout = QHBoxLayout()
        layout.addWidget(self.sidebar)

        right_panel = QVBoxLayout()
        right_panel.addWidget(QLabel("Port Forwarding"))
        right_panel.addWidget(self.port_table)

        # Add input fields and button for new port forwarding

        self.port_input_layout = QHBoxLayout()
        self.local_port_input = QLineEdit()
        self.local_port_input.setPlaceholderText("Local Port")
        self.remote_port_input = QLineEdit()
        self.remote_port_input.setPlaceholderText("Remote Port")
        self.add_port_btn = QPushButton("Add Port")
        self.add_port_btn.clicked.connect(self.add_new_port)

        self.port_input_layout.addWidget(self.local_port_input)
        self.port_input_layout.addWidget(self.remote_port_input)
        self.port_input_layout.addWidget(self.add_port_btn)

        right_panel.addLayout(self.port_input_layout)

        layout.addLayout(right_panel)

        self.setLayout(layout)

        self.setStyleSheet("""
            QWidget {
                background-color: #1e1e1e;
                color: white;
                font-family: 'Segoe UI', sans-serif;
                font-size: 14px;
            }
            QLineEdit, QListWidget, QTableWidget {
                background-color: #2d2d30;
                color: white;
                border: 1px solid #3e3e42;
                padding: 4px;
            }
            QPushButton {
                background-color: #007acc;
                color: white;
                border: none;
                padding: 6px 12px;
            }
            QPushButton:hover {
                background-color: #005f9e;
            }
            QLabel {
                font-weight: bold;
                font-size: 16px;
            }
        """)

        self.hosts = parse_ssh_config()
        self.load_hosts()

        self.timer = QTimer()
        self.timer.timeout.connect(self.update_active_status)
        self.timer.start(1000)

    def load_hosts(self):
        for host in self.hosts:
            self.sidebar.addItem(host)

    def load_host_ports(self, item):
        hostname = item.text()
        self.port_table.setRowCount(0)
        ports = self.tunnel_manager.get_ports_for_host(hostname)

        for port in ports:
            row = self.port_table.rowCount()
            self.port_table.insertRow(row)
            item_local = QTableWidgetItem(str(port["local"]))
            item_local.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            item_remote = QTableWidgetItem(str(port["remote"]))
            item_remote.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.port_table.setItem(row, 0, item_local)
            self.port_table.setItem(row, 1, item_remote)
            btn = QPushButton("Stop" if port["active"] else "Start")
            btn.clicked.connect(lambda _, h=hostname, p=port: self.toggle_tunnel(h, p))
            self.port_table.setCellWidget(row, 2, btn)

            delete_btn = QPushButton("Delete")
            delete_btn.clicked.connect(lambda _, h=hostname, p=port: self.delete_port(h, p))
            self.port_table.setCellWidget(row, 3, delete_btn)

    def toggle_tunnel(self, host, port):
        if port["active"]:
            self.tunnel_manager.stop_tunnel(host, port)
        else:
            self.tunnel_manager.start_tunnel(host, port)
        self.load_host_ports(self.sidebar.currentItem())

    def update_active_status(self):
        for i in range(self.sidebar.count()):
            item = self.sidebar.item(i)
            host = item.text()
            active = self.tunnel_manager.is_any_tunnel_active(host)
            item.setForeground(Qt.GlobalColor.green if active else Qt.GlobalColor.black)

    def add_new_port(self):
        current_item = self.sidebar.currentItem()
        if not current_item:
            return
        host = current_item.text()
        try:
            local_port = int(self.local_port_input.text())
            remote_port = int(self.remote_port_input.text())
        except ValueError:
            return  # optionally show a message box for invalid input
        self.tunnel_manager.add_port_forward(host, local_port, remote_port)
        self.load_host_ports(current_item)
        self.local_port_input.clear()
        self.remote_port_input.clear()

    def delete_port(self, host, port):
        self.tunnel_manager.stop_tunnel(host, port)
        self.tunnel_manager.remove_port_forward(host, port)
        self.load_host_ports(self.sidebar.currentItem())

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SSHTunnelApp()
    window.show()
    sys.exit(app.exec())