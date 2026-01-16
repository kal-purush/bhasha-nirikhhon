import os
import subprocess
import sys
import logging
import platform
import shutil

# Thiết lập logging
logging.basicConfig(
    filename='build.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def run_command(command: list, description: str) -> bool:
    """Chạy lệnh shell và xử lý lỗi."""
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logging.info(f"{description} thành công: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Lỗi khi {description}: {e.stderr}")
        print(f"Lỗi: {description} thất bại. Kiểm tra build.log để biết chi tiết.")
        return False
    except Exception as e:
        logging.error(f"Lỗi không xác định khi {description}: {e}")
        return False

def install_requirements():
    """Cài đặt các thư viện phụ thuộc."""
    requirements = [
        'pyinstaller',
        'pandas',
        'openpyxl',
        'requests',
        'inputimeout'
    ]
    for pkg in requirements:
        if not run_command([sys.executable, '-m', 'pip', 'install', pkg], f"Cài đặt {pkg}"):
            return False
    return True

def clean_build_cache(project_root: str):
    """Xóa thư mục build, dist và các file .spec để tránh cache."""
    for dir_name in ['build', 'dist']:
        dir_path = os.path.join(project_root, dir_name)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            logging.info(f"Đã xóa thư mục {dir_path}")
    for file in os.listdir(project_root):
        if file.endswith('.spec'):
            os.remove(os.path.join(project_root, file))
            logging.info(f"Đã xóa file {file}")

def build_executable(project_root: str):
    """Đóng gói ứng dụng thành file thực thi."""
    main_script = os.path.join(project_root, 'WorkflowManager.py')
    output_dir = os.path.join(project_root, 'dist')
    icon_path = None  # Thay bằng đường dẫn icon nếu có (ví dụ: 'icon.ico')

    if not os.path.exists(main_script):
        logging.error(f"Không tìm thấy {main_script}")
        print(f"Lỗi: Không tìm thấy {main_script}")
        return False

    # Xác định dấu phân cách cho --add-data dựa trên hệ điều hành
    separator = ';' if platform.system() == 'Windows' else ':'

    # Lệnh PyInstaller
    command = [
        'pyinstaller',
        '--onefile',  # Đóng gói thành một file
        '--distpath', output_dir,
        '--workpath', os.path.join(project_root, 'build'),
        '--specpath', os.path.join(project_root, 'build'),
        '--clean',  # Xóa cache trước khi build
        main_script
    ]

    # Thêm icon nếu có
    if icon_path and os.path.exists(icon_path):
        command.extend(['--icon', icon_path])

    # Thêm các file mã nguồn liên quan
    related_files = [
        'TelegramNotifier.py',
        'AgentConfig.py',
        'GoogleSheetDownloader.py',
        'ExcelSnapshotComparator.py',
        'ReportGenerator.py'
    ]
    missing_files = []
    for related_file in related_files:
        file_path = os.path.join(project_root, related_file)
        if os.path.isfile(file_path):
            command.extend(['--add-data', f"{file_path}{separator}."])
            logging.info(f"Đã thêm {file_path} vào lệnh PyInstaller")
        else:
            missing_files.append(related_file)
            logging.warning(f"Không tìm thấy {file_path} trong thư mục dự án, bỏ qua.")

    if missing_files:
        print(f"Cảnh báo: Các file sau không tồn tại: {', '.join(missing_files)}")
        print("Đảm bảo các file này tồn tại nếu cần thiết.")

    if not run_command(command, "Đóng gói ứng dụng với PyInstaller"):
        return False

    # Kiểm tra file thực thi
    executable_name = 'WorkflowManager.exe' if sys.platform == 'win32' else 'WorkflowManager'
    executable_path = os.path.join(output_dir, executable_name)
    if os.path.exists(executable_path):
        logging.info(f"Đã tạo file thực thi: {executable_path}")
        print(f"Thành công! File thực thi: {executable_path}")
        print("Lưu ý: Copy project_config.json và workflow_config.json vào cùng thư mục với file thực thi trên máy đích.")
        return True
    else:
        logging.error(f"Không tìm thấy file thực thi tại {executable_path}")
        print(f"Lỗi: Không tạo được file thực thi")
        return False

def main():
    """Chạy build script."""
    # Lấy thư mục gốc của dự án (thư mục chứa build.py hoặc thư mục cha)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir) if os.path.basename(current_dir) == 'utils' else current_dir

    print(f"Bắt đầu build ứng dụng từ thư mục: {project_root}")
    logging.info(f"Bắt đầu quá trình build từ thư mục: {project_root}")

    # Xóa cache trước khi build
    clean_build_cache(project_root)

    # Cài đặt phụ thuộc
    if not install_requirements():
        logging.error("Cài đặt phụ thuộc thất bại")
        print("Lỗi: Cài đặt phụ thuộc thất bại")
        sys.exit(1)

    # Đóng gói ứng dụng
    if not build_executable(project_root):
        logging.error("Đóng gói ứng dụng thất bại")
        print("Lỗi: Đóng gói ứng dụng thất bại")
        sys.exit(1)

    print("Hoàn tất build! Kiểm tra thư mục 'dist' để lấy file thực thi.")
    logging.info("Hoàn tất build")

if __name__ == "__main__":
    main()