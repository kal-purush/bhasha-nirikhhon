import keyboard
import pyautogui
import tkinter as tk
import time

base_image_path = 'fig/base_image.png'
book_image_path = 'fig/book_btn.png'
empty_image_path = 'fig/empty_btn.png'
booked_image_path = 'fig/booked_btn.png'
washing_image_path = 'fig/washing_btn.png'
success_booked_image_path = 'fig/success_booked_btn.png'

class BathBookingSystem:
    def __init__(self, nums_windows):
        pyautogui.FAILSAFE = True
        pyautogui.PAUSE = 1.0
        
        self.base_image_path = base_image_path
        self.book_image_path = book_image_path
        self.empty_image_path = empty_image_path
        self.booked_image_path = booked_image_path
        self.washing_image_path = washing_image_path
        self.success_booked_image_path = success_booked_image_path
        
        self.nums_windows = nums_windows
        self.base_locations = {}
        self.drawing = False
        self.start_x = None
        self.start_y = None
        
        self.base_pos = self.locate_base_image()
        self.book_btn_pos = self.locate_btn(self.book_image_path)

    def locate_base_image(self):
        """定位基准图像"""
        try:
            base_position = pyautogui.locateOnScreen(self.base_image_path, confidence=0.9)
            if base_position:
                return (base_position.left, base_position.top)
            return None
        except Exception as e:
            if e.__class__.__name__ == "ImageNotFoundException":
                print(f"无法找到基准图像: {self.base_image_path}, 请确保桌面上有该图像")
            elif e.__class__.__name__ == "FileNotFoundError":
                print(f"找不到文件: {self.base_image_path}")
            else:
                print(f"未知错误: {e}")
            exit(1)
    
    def locate_btn(self, btn_image_path):
        """定位按钮"""
        try:
            btn_position = pyautogui.locateOnScreen(btn_image_path, confidence=0.9)
            if btn_position:
                return (btn_position.left, btn_position.top)
            return None
        except Exception as e:
            if e.__class__.__name__ == "ImageNotFoundException":
                print(f"无法找到按钮: {btn_image_path}, 请确保桌面上有该按钮")
            elif e.__class__.__name__ == "FileNotFoundError":
                print(f"找不到文件: {btn_image_path}")
            else:
                print(f"未知错误: {e}")
            exit(1)

    def on_mouse_down(self, event):
        self.drawing = True
        self.start_x = event.x
        self.start_y = event.y

    def on_mouse_move(self, event, canvas):
        if self.drawing:
            canvas.delete("rect")
            canvas.create_rectangle(
                self.start_x, self.start_y, event.x, event.y,
                outline='red', tags="rect"
            )

    def on_mouse_up(self, event, canvas, window, idx):
        self.drawing = False
        end_x, end_y = event.x, event.y
        
        relative_pos = (
            self.start_x - self.base_pos[0],
            self.start_y - self.base_pos[1]
        )
        self.base_locations[idx] = {
            'relative_pos': relative_pos,
            'size': (end_x - self.start_x, end_y - self.start_y)
        }
        print(f"记录位置: {relative_pos}")
        
        window.destroy()

    def draw_selection(self, idx):
        """允许用户在屏幕上画框"""
        window = tk.Tk()
        window.attributes('-fullscreen', True, '-alpha', 0.3)
        window.configure(background='white')

        canvas = tk.Canvas(window, highlightthickness=0)
        canvas.pack(fill='both', expand=True)

        canvas.bind("<Button-1>", self.on_mouse_down)
        canvas.bind("<B1-Motion>", lambda event: self.on_mouse_move(event, canvas))
        canvas.bind("<ButtonRelease-1>", lambda event: self.on_mouse_up(event, canvas, window, idx))

        window.mainloop()

    def match_and_click(self, location_name):
        """匹配位置并点击"""
        if not self.base_pos or location_name not in self.base_locations.keys():
            return False

        location = self.base_locations[location_name]
        target_x = self.base_pos[0] + location['relative_pos'][0]
        target_y = self.base_pos[1] + location['relative_pos'][1]

        # 点击中心位置
        click_x = target_x + location['size'][0] // 2
        click_y = target_y + location['size'][1] // 2
        pyautogui.click(click_x, click_y)
        
        pyautogui.click(self.book_btn_pos[0], self.book_btn_pos[1])
        try:
            if pyautogui.locateOnScreen(self.success_booked_image_path, confidence=0.9):
                print(f"已预约成功")
                return True
        except Exception as e:
            print(f"未找到预约成功按钮")
            return False

def main():
    nums_windows = int(input("请输入需要预约的窗口数量: "))
    
    booking_system = BathBookingSystem(nums_windows=nums_windows)
    for idx in range(nums_windows):
        print(f"请框选第 {idx+1} 个位置...")
        booking_system.draw_selection(idx)    

    # 等待用户准备
    print("准备开始预约, 请不要操作鼠标并确保程序在前台, 按下 Ctrl + q 可以停止程序")
    time.sleep(2)
    print("开始匹配并点击...")
    idx = 0
    stop_flag = False
    
    def stop_program():
        nonlocal stop_flag
        stop_flag = True
        print("停止程序")
    
    keyboard.add_hotkey('ctrl+q', stop_program)
    
    while True:
        if stop_flag or booking_system.match_and_click(idx):
            break
        print(f"第 {idx+1} 个位置预约失败, 尝试下一个位置...")
        idx = (idx + 1) % nums_windows
        time.sleep(0.5)
    
    if not stop_flag:
        print(f"预约结束, 开始时间为{time.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()