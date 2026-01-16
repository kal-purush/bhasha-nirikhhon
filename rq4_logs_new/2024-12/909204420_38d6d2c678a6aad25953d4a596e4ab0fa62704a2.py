from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QListWidget,
                             QPushButton, QLabel, QFileDialog, QMessageBox,
                             QInputDialog, QWidget, QFrame)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPixmap, QColor
from PIL import Image, ImageQt
import os


class ColorButton(QPushButton):
    def __init__(self, color_name, rgb, is_dark_mode=False):
        super().__init__()
        self.setFixedSize(30, 30)
        self.setCheckable(True)
        self.color_name = color_name
        self.rgb = rgb
        self.update_style(is_dark_mode)

    def update_style(self, is_dark_mode):
        border_color = "#555555" if is_dark_mode else "#cccccc"
        hover_color = "#666666" if is_dark_mode else "#3daee9"
        self.setStyleSheet(f"""
            QPushButton {{
                background-color: {self.color_name};
                border: 2px solid {border_color};
                border-radius: 15px;
            }}
            QPushButton:checked {{
                border: 2px solid {hover_color};
            }}
            QPushButton:hover {{
                border: 2px solid {hover_color};
            }}
        """)


class GifMakerDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("PNG to Animated GIF Converter")
        self.setMinimumSize(800, 600)

        # Get theme from parent
        self.parent_window = parent
        self.is_dark_mode = self.parent_window.theme_manager.get_theme() == "dark"

        # Initialize variables
        self.image_list = []
        self.delay = 1000
        self.bg_color = (255, 255, 255)  # Default white background

        self.setup_ui()
        self.apply_theme()

    def setup_ui(self):
        # Main layout
        main_layout = QHBoxLayout(self)
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # Left panel (list and color options)
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setSpacing(15)

        # Files section
        files_label = QLabel("Image Files")
        files_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        left_layout.addWidget(files_label)

        # List widget
        self.list_widget = QListWidget()
        left_layout.addWidget(self.list_widget)

        # Color picker section
        color_section = QWidget()
        color_layout = QVBoxLayout(color_section)
        color_layout.setSpacing(10)

        color_label = QLabel("Background Color")
        color_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        color_layout.addWidget(color_label)

        # Color buttons in a grid
        color_grid = QHBoxLayout()
        color_grid.setSpacing(10)

        self.colors = [
            ('white', (255, 255, 255)),
            ('black', (0, 0, 0)),
            ('blue', (0, 0, 255)),
            ('red', (255, 0, 0)),
            ('green', (0, 255, 0)),
            ('gray', (128, 128, 128))
        ]

        self.color_buttons = []
        for name, rgb in self.colors:
            btn = ColorButton(name, rgb, self.is_dark_mode)
            btn.clicked.connect(lambda checked, c=rgb: self.set_bg_color(c))
            if rgb == self.bg_color:
                btn.setChecked(True)
            color_grid.addWidget(btn)
            self.color_buttons.append(btn)

        color_grid.addStretch()
        color_layout.addLayout(color_grid)
        left_layout.addWidget(color_section)

        # Add left panel to main layout
        main_layout.addWidget(left_panel, stretch=2)

        # Right panel (buttons)
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setSpacing(10)

        # File management buttons
        file_buttons = QWidget()
        file_buttons_layout = QVBoxLayout(file_buttons)
        file_buttons_layout.setSpacing(8)

        self.add_button = QPushButton("Add PNG Files")
        self.add_button.clicked.connect(self.add_files)
        file_buttons_layout.addWidget(self.add_button)

        self.remove_button = QPushButton("Remove Selected")
        self.remove_button.clicked.connect(self.remove_selected)
        file_buttons_layout.addWidget(self.remove_button)

        right_layout.addWidget(file_buttons)

        # Order buttons
        order_buttons = QWidget()
        order_buttons_layout = QVBoxLayout(order_buttons)
        order_buttons_layout.setSpacing(8)

        self.move_up_button = QPushButton("Move Up")
        self.move_up_button.clicked.connect(self.move_up)
        order_buttons_layout.addWidget(self.move_up_button)

        self.move_down_button = QPushButton("Move Down")
        self.move_down_button.clicked.connect(self.move_down)
        order_buttons_layout.addWidget(self.move_down_button)

        right_layout.addWidget(order_buttons)

        # Settings and conversion buttons
        action_buttons = QWidget()
        action_buttons_layout = QVBoxLayout(action_buttons)
        action_buttons_layout.setSpacing(8)

        self.delay_button = QPushButton("Set Delay (ms)")
        self.delay_button.clicked.connect(self.set_delay)
        action_buttons_layout.addWidget(self.delay_button)

        self.convert_button = QPushButton("Convert to GIF")
        self.convert_button.clicked.connect(self.convert_to_gif)
        action_buttons_layout.addWidget(self.convert_button)

        right_layout.addWidget(action_buttons)
        right_layout.addStretch()

        # Add right panel to main layout
        main_layout.addWidget(right_panel, stretch=1)

    def apply_theme(self):
        """Apply the current theme to all widgets"""
        is_dark = self.is_dark_mode

        # Colors for dark/light themes
        bg_color = "#2e2e2e" if is_dark else "#ffffff"
        text_color = "#ffffff" if is_dark else "#000000"
        list_bg = "#3c3c3c" if is_dark else "#ffffff"
        border_color = "#555555" if is_dark else "#cccccc"
        button_bg = "#424242" if is_dark else "#f0f0f0"
        button_hover = "#4f4f4f" if is_dark else "#e0e0e0"
        accent_color = "#3daee9"

        # Update dialog background
        self.setStyleSheet(f"""
            QDialog {{
                background-color: {bg_color};
                color: {text_color};
            }}
            QLabel {{
                color: {text_color};
            }}
        """)

        # Update list widget
        self.list_widget.setStyleSheet(f"""
            QListWidget {{
                background-color: {list_bg};
                border: 1px solid {border_color};
                border-radius: 4px;
                color: {text_color};
            }}
            QListWidget::item {{
                padding: 5px;
                border-radius: 2px;
            }}
            QListWidget::item:selected {{
                background-color: {accent_color};
                color: white;
            }}
            QListWidget::item:hover {{
                background-color: {"#4a4a4a" if is_dark else "#f0f0f0"};
            }}
        """)

        # Button style
        button_style = f"""
            QPushButton {{
                background-color: {button_bg};
                color: {text_color};
                border: 1px solid {border_color};
                border-radius: 4px;
                padding: 8px 16px;
                min-width: 120px;
            }}
            QPushButton:hover {{
                background-color: {button_hover};
                border: 1px solid {accent_color};
            }}
            QPushButton:pressed {{
                background-color: {"#383838" if is_dark else "#d0d0d0"};
            }}
        """

        # Apply button style to all buttons
        for button in [self.add_button, self.remove_button,
                       self.move_up_button, self.move_down_button,
                       self.delay_button]:
            button.setStyleSheet(button_style)

        # Special style for convert button
        self.convert_button.setStyleSheet(f"""
            QPushButton {{
                background-color: {accent_color};
                color: white;
                border: none;
                border-radius: 4px;
                padding: 8px 16px;
                min-width: 120px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: #3498db;
            }}
            QPushButton:pressed {{
                background-color: #2980b9;
            }}
        """)

        # Update color buttons
        for btn in self.color_buttons:
            btn.update_style(is_dark)
    def set_bg_color(self, color):
        """Set the background color for GIF conversion"""
        self.bg_color = color
        # Update button states
        for btn in self.color_buttons:
            btn.setChecked(btn.rgb == color)

    def normalize_images(self, images):
        """Normalize all images to the same size with the selected background"""
        try:
            # Find maximum dimensions
            max_width = max(img.size[0] for img in images)
            max_height = max(img.size[1] for img in images)

            normalized_images = []
            for img in images:
                # Calculate scaling factor while maintaining aspect ratio
                width_ratio = max_width / img.size[0]
                height_ratio = max_height / img.size[1]
                scale_factor = min(width_ratio, height_ratio)

                # Scale the image
                new_width = int(img.size[0] * scale_factor)
                new_height = int(img.size[1] * scale_factor)
                scaled_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

                # Create new image with max dimensions and background color
                new_img = Image.new('RGB', (max_width, max_height), self.bg_color)

                # Calculate position to center the scaled image
                left = (max_width - new_width) // 2
                top = (max_height - new_height) // 2

                # Paste the scaled image onto the new background
                new_img.paste(scaled_img, (left, top))
                normalized_images.append(new_img)

            return normalized_images
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to normalize images: {str(e)}")
            return None

    def add_files(self):
        """Add PNG files to the list"""
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select PNG Files",
            "",
            "PNG Files (*.png)"
        )

        for file in files:
            if file not in self.image_list:
                self.image_list.append(file)
                self.list_widget.addItem(os.path.basename(file))

    def remove_selected(self):
        """Remove selected file from the list"""
        current = self.list_widget.currentRow()
        if current >= 0:
            self.list_widget.takeItem(current)
            self.image_list.pop(current)

    def move_up(self):
        """Move selected item up in the list"""
        current = self.list_widget.currentRow()
        if current > 0:
            item = self.list_widget.takeItem(current)
            self.list_widget.insertItem(current - 1, item)
            self.list_widget.setCurrentRow(current - 1)
            self.image_list[current], self.image_list[current - 1] = \
                self.image_list[current - 1], self.image_list[current]

    def move_down(self):
        """Move selected item down in the list"""
        current = self.list_widget.currentRow()
        if current < self.list_widget.count() - 1:
            item = self.list_widget.takeItem(current)
            self.list_widget.insertItem(current + 1, item)
            self.list_widget.setCurrentRow(current + 1)
            self.image_list[current], self.image_list[current + 1] = \
                self.image_list[current + 1], self.image_list[current]

    def set_delay(self):
        """Set delay between frames"""
        delay, ok = QInputDialog.getInt(
            self,
            "Set Delay",
            "Enter delay between frames (ms):",
            value=self.delay,
            min=100,
            max=10000,
            step=100
        )
        if ok:
            self.delay = delay

    def convert_to_gif(self):
        """Convert selected PNG files to animated GIF"""
        if not self.image_list:
            QMessageBox.warning(self, "Warning", "No images selected.")
            return

        try:
            output_file, _ = QFileDialog.getSaveFileName(
                self,
                "Save GIF",
                "",
                "GIF Files (*.gif)"
            )

            if not output_file:
                return

            # Load and normalize images
            original_images = []
            for file in self.image_list:
                img = Image.open(file)
                img = img.convert('RGB')
                original_images.append(img)

            normalized_images = self.normalize_images(original_images)
            if not normalized_images:
                return

            # Save as animated GIF
            normalized_images[0].save(
                output_file,
                save_all=True,
                append_images=normalized_images[1:],
                duration=self.delay,
                loop=0,
                optimize=False
            )

            QMessageBox.information(self, "Success", f"GIF saved as {output_file}")
            self.preview_gif(output_file)

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to create GIF: {str(e)}")

    def preview_gif(self, gif_file):
        """Show GIF preview in a new dialog"""
        try:
            preview_dialog = QDialog(self)
            preview_dialog.setWindowTitle("GIF Preview")
            preview_layout = QVBoxLayout(preview_dialog)

            # Create label for the GIF
            label = QLabel()
            preview_layout.addWidget(label)

            # Load the GIF frames
            img = Image.open(gif_file)
            frames = []

            try:
                while True:
                    # Convert PIL image to QPixmap for display
                    current_frame = ImageQt.ImageQt(img)
                    pixmap = QPixmap.fromImage(current_frame)
                    frames.append(pixmap)
                    img.seek(len(frames))
            except EOFError:
                pass

            if not frames:
                QMessageBox.critical(self, "Error", "No frames found in GIF")
                preview_dialog.close()
                return

            current_frame = 0

            def update_frame():
                nonlocal current_frame
                if not preview_dialog.isVisible():
                    return

                try:
                    label.setPixmap(frames[current_frame])
                    current_frame = (current_frame + 1) % len(frames)
                    preview_dialog.frame_timer = self.startTimer(self.delay)
                except Exception:
                    preview_dialog.close()

            # Handle timer events for frame updates
            def timerEvent(event):
                update_frame()

            preview_dialog.timerEvent = timerEvent
            preview_dialog.frame_timer = preview_dialog.startTimer(self.delay)

            # Start with first frame
            if frames:
                label.setPixmap(frames[0])

            preview_dialog.exec()
        except Exception as e:
            print(e)