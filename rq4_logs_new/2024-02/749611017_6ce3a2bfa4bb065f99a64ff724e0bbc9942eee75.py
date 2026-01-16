import os

from alembic import command
from alembic.config import Config

from util.file import ConfigManager
from util.proc import run_command_in_directory

env_content = """
import os
import sys
sys.path.append(os.path.realpath("%s"))
import models
target_metadata = models.Base.metadata
"""


def init_alembic(init_dir, ini_path):
    """
    Initialize the alembic 仓库
    @param init_dir: alembic文件夹存放路径 如 /_alembi/alembic
    @param ini_path:  alembic.ini存放路径 如 /_alembic/alembic.ini
    @return: None
    """
    config = Config(ini_path)
    command.init(config, init_dir, template="generic")


def set_section_value_in_ini(ini_path, section, option, value):
    """
    设置ini文件中的某个section和option的值
    如: set_section_value_in_ini(ini_path, 'alembic', 'sqlalchemy.url', sqlalchemy_url)
    @param ini_path: alembic.ini存放路径 如 /_alembic/alembic.ini
    @param section:
    @param option: 操作的key
    @param value:  修改后的值
    """
    manager = ConfigManager(ini_path)
    manager.set_value(section, option, value)


def env_content_update(env_dir, sys_path):
    data = env_content % sys_path
    # 打开源文件（以只读方式）
    env_path = env_dir + '/env.py'
    env_temp = env_dir + '/temp_env.py'
    with open(env_path, 'r') as read_file:
        # 创建一个临时文件用于保存修改后的数据
        with open(env_temp, 'w') as write_file:
            for line in read_file:
                # 检查当前行是否以' target_metadata'开头
                if line.startswith('target_metadata'):
                    # 如果是，则写入新的内容
                    write_file.write(data)
                else:
                    # 不是则保持原样写入
                    write_file.write(line)
    # 确保所有内容已成功写入临时文件
    # 然后替换原来的文件
    os.replace(env_temp, env_path)


def autogenerate(execution_directory):
    run_command_in_directory(execution_directory, 'alembic revision --autogenerate')
    run_command_in_directory(execution_directory, 'alembic upgrade head')


def whole_process(alembic_dir, alembic_ini_path, obj_path, sqlalchemy_addr):
    init_alembic(alembic_dir, alembic_ini_path)
    set_section_value_in_ini(alembic_ini_path, 'alembic', 'sqlalchemy.url', sqlalchemy_addr)
    env_content_update(alembic_dir, obj_path)


if __name__ == '__main__':
    init_dir = '/xxx/squirrellib/_alembic/alembic'
    ini_path = '/xxx/squirrellib/_alembic/alembic.ini'
    sys_path = "/Users/wushuaikang/Desktop/code/squirrellib/squirrellib"
    sqlalchemy_url = 'postgresql+psycopg2://postgres:postgres@127.0.0.1/postgres'
    whole_process(init_dir, ini_path, sys_path, sqlalchemy_url)
# 1.alembic init alembic  创建alembic.ini文件和alembic的文件夹 最后一个alembic为生成文件的名字
# 2.修改alembic.ini文件 在文件中添加一行version_path_separator = sqlalchemy.url
# 3.修改env.py文件 修改target_metadata=None,先导入自己的model文件将target_metadata修改为自己的models里的文件
# 4.使用命令alembic revision --autogenerate -m "备注" 生成当前的版本
# 5.使用命令alembic upgrade head 升级到最新版本