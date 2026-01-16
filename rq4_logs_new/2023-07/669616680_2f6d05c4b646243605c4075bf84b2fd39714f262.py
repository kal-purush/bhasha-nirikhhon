import playwright
import  time
# from langchain.chat_models import ChatOpenAI
# from langchain.schema import SystemMessage, BaseMessage, AIMessage


from playwright.sync_api import Playwright, sync_playwright, expect




def auto_login(playwright: Playwright, auth_json_path=None): 
    '''uses cookies to loigg'''

    url = 'https://twitter.com/home'

    browser = playwright.chromium.launch(headless=False, timeout=60000)
    context = browser.new_context(storage_state=auth_json_path)
    page = context.new_page()
    page.goto(url)

    return browser, page
    

def manual_login(playwright: Playwright, auth_json_path=None): 

    url = 'https://twitter.com/i/flow/login'
    username = 'mr_beast_bot'
    password = 'P6yKTy0Q4ZJ#7Mml'
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context(storage_state=auth_json_path)
    page = context.new_page()
    page.goto(url)
    page.get_by_label("Phone, email, or username").click()
    page.get_by_label("Phone, email, or username").fill(username)
    page.get_by_role("button", name="Next").click()
    time.sleep(3)
    page.get_by_label("Password", exact=True).click()
    page.get_by_label("Password", exact=True).fill(password)
    page.get_by_test_id("LoginForm_Login_Button").click()
    time.sleep(3)
    page.goto('https://twitter.com/home')
    
    return browser, page


def scrape_webpage(page, taraget_url, file_name='target.html'):

    page.goto(taraget_url)
    time.sleep(3) # just to make sure things load
    html = page.content()
    # save html
    with open(file_name, 'w') as file:
        file.write(html)



def create_and_send_tweet(page, handle: str):

    default_msg = """
    {handle} I am a MR. BEAST AGENT. I WANT TO GIVE MONEY TO GOOD PEOPLE. 
    """
    customized_msg = default_msg.format(handle=handle)

    page.get_by_test_id("tweetTextarea_0").fill(customized_msg)
    time.sleep(3)
    page.get_by_test_id("tweetButtonInline").click()
    time.sleep(3)

    return page
    

def send_initial_tweet(handle: str): 

    with sync_playwright() as playwright:
        browser, page = manual_login(playwright, auth_json_path=None)


        page = create_and_send_tweet(page, handle)

    return page


if __name__ == "__main__":

    handle= "@lina_colucci"
    send_initial_tweet(handle)


