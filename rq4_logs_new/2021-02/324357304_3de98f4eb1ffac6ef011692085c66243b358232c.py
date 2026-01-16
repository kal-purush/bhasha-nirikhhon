import allure
import logging
from common.common_fun import get_picture_data


logging.basicConfig(level=logging.INFO)


def black_wrapper(fun):
    """
    处理黑名单的装饰器 \n
    :param fun: 使用装饰器的函数
    :return: 使用装饰器的函数
    """
    def run(*args, **kwargs):
        base_page = args[0]
        try:
            logging.info(f"start find: \nargs: {str(args)} kwargs: {str(kwargs)}")
            return fun(*args, **kwargs)
        except Exception as e:
            base_page.screenshot("tmp.png")
            picture_data = get_picture_data(base_page.latest_screenshot("../screenshots"))
            allure.attach(picture_data, attachment_type=allure.attachment_type.PNG)
            for black in base_page.black_list:
                elements = base_page.finds(*black)
                if len(elements) > 0:
                    elements[0].click()
                    return fun(*args, **kwargs)
            raise e
    return run