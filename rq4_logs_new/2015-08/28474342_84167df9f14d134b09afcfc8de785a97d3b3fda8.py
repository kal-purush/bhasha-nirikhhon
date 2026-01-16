
blackCog = """
QWidget {
			background-color: rgb(40, 40, 40);
			color: rgb(180, 180, 180);
			font-size:12px;
		}

QPushButton {
				background-color: qlineargradient( x1:0 y1:0, x2:0 y2:1, stop:0 #3F3F3F, stop:1 #353535 );
				color: rgb(150, 150, 150);
				border-style: solid;
				border-width: 1px;
				border-radius: 3px;
				border-color: rgb(30, 30, 30);
			}
QPushButton:hover 	{
						background-color: qlineargradient( x1:0 y1:0, x2:0 y2:1, stop:0 #336088, stop:1 #225077 );
						color: rgb(180, 180, 180);
					}
QPushButton:pressed {
						border-color: rgb(25, 25, 25);
						background-color: rgb(25, 25, 25);
					}

QLineEdit 	{
				background-color: rgb(60, 60, 60);
				border-style: solid;
				border-width: 1px;
				border-radius: 3px;
				border-color: rgb(30, 30, 30);
			}

QTabWidget:pane {
					border-style: solid;
					border-width: 0px;
					border-top-width: 1px;
					border-radius: 3px;
					border-color: rgb(30, 30, 30);
				}
QTabBar:tab {
				border-style: solid;
				border-width: 1px;
				border-bottom-width: 1px;
				border-radius: 3px;
				border-color: rgb(30, 30, 30);
				background: rgb(25, 25, 25);
				margin-top: 5px;
				padding: 3px 15px 2px 15px;
			}
QTabBar:tab:selected 	{
							background: rgb(40, 40, 40);
							margin-top: 3px;
						}

QGroupBox 	{
				border-style: solid;
				border-width: 1px;
				border-radius: 3px;
				border-color: rgb(30, 30, 30);
			}

QTreeView	{
				border-style: solid;
				border-width: 1px;
				border-radius: 3px;
				border-color: rgb(30, 30, 30);
				background: rgb(60, 60, 60);
			}
QTreeView:item:hover 	{
							background: rgb(30, 50, 80);
						}
QTreeView:item:selected {
							background: rgb(40, 80, 120);
							border: 0px;
						}

QComboBox 	{
				border-style: solid;
				border-width: 1px;
				border-radius: 3px;
				border-color: rgb(30, 30, 30);
				background-color: qlineargradient( x1:0 y1:0, x2:0 y2:1, stop:0 #3F3F3F, stop:1 #353535 );
			}
QComboBox:down-arrow 	{
							padding: 5px;
						}
QComboBox QListView {
						border: 0px;
						background: rgb(60, 60, 60);
					}
QComboBox:drop-down
{
	width: 16px;
}

QScrollBar:sub-page {
						background: rgb(30, 30, 30);
					}
QScrollBar:add-page {
						background: rgb(30, 30, 30);
					}
QScrollBar:handle 	{
						background: rgb(80, 80, 80);
					}

QMenuBar:item 	{
					width: 60px;
					height: 25px;
					background: rgb(60, 60, 60);
				}
QMenuBar:item:selected 	{
							background: rgb(30, 50, 80);
						}
QMenuBar:item:pressed 	{
							background: rgb(40, 80, 120);
						}
QMenu:item:selected {
						background: rgb(30, 50, 80);
					}
QMenu:item:pressed 	{
						background: rgb(40, 80, 120);
					}
"""