import vk_api
import os
from dotenv import load_dotenv
import logging
from logger_config import logger


load_dotenv()

class ParsingVK():
    def __init__(self, count=None):
        ACCESS_TOKEN = os.getenv("SERVICE_KEY")
        GROUPS_ID = self.loading_group_ids()
        self.check_tokens(ACCESS_TOKEN, GROUPS_ID[27])
        VERSION_VK_API = 5.199
        self.count_posts = count if count is not None else 3
        vk_session = vk_api.VkApi(token=ACCESS_TOKEN)
        vk = vk_session.get_api()
        self.response = vk.wall.get(domain=GROUPS_ID[27], v=VERSION_VK_API, count=self.count_posts)

        self.post_list = []

    def loading_group_ids(self):
        with open("groupsID_for_parsing.txt", "r", encoding="utf-8") as file:
            lines = [line.strip().rsplit("/", 1)[-1] for line in file]
        return lines
    

    def check_tokens(self, ACCESS_TOKEN, GROUPS_ID):
        if not ACCESS_TOKEN:
            message = f"Отсутствует обязательная переменная окружения {ACCESS_TOKEN}"
            logger.critical(message)
            raise ValueError("Нет переменной окружения SERVICE_KEY")
        if not GROUPS_ID:
            message = f"Отсутствует обязательная переменная окружения {ACCESS_TOKEN}"
            logger.critical(message)
            raise ValueError("Нет переменной окружения GROUP_ID")


    def check_response():
        pass



    def filter_content(self) -> list[dict]:
        '''
        '''

        #Пришли валидные данные
        for i in range(self.count_posts):
            post = {}
            post_text = self.response["items"][i]["text"]
            post["id"] = i
            post["text"] = post_text
            self.post_list.append(post)
        return self.post_list
    

    def get_filter_data(self):
        return self.post_list
    
    def __repr__(self):
        pass
    
if __name__ == "__main__":
    parser_vk = ParsingVK(3)
    post_list = parser_vk.filter_content()
    for i in range(len(post_list)):
        print("-----------------------------")
        print(post_list[i])
    logger.info("Выполнилось")