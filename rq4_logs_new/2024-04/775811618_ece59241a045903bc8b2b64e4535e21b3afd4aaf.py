from PIL import Image

def scan_grid_image(image_path):
    # 載入圖片
    with Image.open(image_path) as img:
        img = img.convert('RGB')  # 確保圖片是RGB格式
        
        width, height = img.size
        total_nodes = width * height
        obstacle_count = 0

        # 掃描每個像素以計算障礙物數量
        for x in range(width):
            for y in range(height):
                pixel = img.getpixel((x, y))
                if pixel != (255, 255, 255):  # 假設非白色像素是障礙物
                    obstacle_count += 1

    return {
        'width': width,
        'height': height,
        'total_nodes': total_nodes,
        'obstacle_nodes': obstacle_count
    }

# 使用範例
image_info = scan_grid_image('/home/jim/Desktop/image_test.png')
print(image_info)