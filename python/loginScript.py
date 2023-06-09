import time
from dotenv import load_dotenv
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from selenium.webdriver.chrome.options import Options


twitch_username= os.getenv("twitch_username")
twitch_password= os.getenv("twitch_password")
# Create Chrome options
chrome_options = Options()

class Browser:
    browser, service = None, None

    # Initialise the webdriver with the path to chromedriver.exe
    def __init__(self, driver: str):
        self.service = Service(driver)
        self.browser = webdriver.Chrome(service=self.service,options=chrome_options)

    def open_page(self, url: str):
        self.browser.get(url)

    def close_browser(self):
        self.browser.close()

    def add_input(self, by: By, value: str, text: str):
        field = self.browser.find_element(by=by, value=value)
        field.send_keys(text)
        time.sleep(1)

    def click_button(self, by: By, value: str):
        button = self.browser.find_element(by=by, value=value)
        button.click()
        time.sleep(1)

    def login_linkedin(self, username: str, password: str):
        self.add_input(by=By.ID, value='login-username', text=username)
        self.add_input(by=By.ID, value='password-input', text=password)
        self.click_button(by=By.CLASS_NAME, value='Layout-sc-1xcs6mc-0')


if __name__ == '__main__':
    browser = Browser('/home/melo/Desktop/DMI/chromedriver_dir/chromedriver')

    browser.open_page('https://www.twitch.tv/login')
    time.sleep(3)

    browser.login_linkedin(username='twitch_username', password='twitch_password')
    time.sleep(10)

