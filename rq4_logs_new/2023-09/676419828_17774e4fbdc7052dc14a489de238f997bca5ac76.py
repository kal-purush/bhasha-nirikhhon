from selenium.webdriver.common.by import By

from src.base_classes.base_page import BasePage


class WebTableElement(BasePage):
    ROWS = (By.CSS_SELECTOR, "tr")
    COLS = (By.CSS_SELECTOR, "tr:first-child>td")

    def __init__(self, browser, locator):
        super().__init__(browser)
        self._webtable = locator

    def get_rows_count(self):
        return len(self.get_elements_from_element(self._webtable, self.ROWS))

    def get_cols_count(self):
        return len(self.get_elements_from_element(self._webtable, self.COLS))

    def get_row_idx_for(self, search_text):
        rows_count = self.get_rows_count()
        cols_count = self.get_cols_count()

        idx = 0
        for row in range(1, rows_count):
            for col in range(1, cols_count):
                if self.get_element_from_element(self._webtable, (By.XPATH, f"//tr[{row}]/td[{col}]")) \
                        .text == search_text:
                    idx = row
                    break
        return idx

    def get_cell_input_element(self, row_idx, col_idx=1, input_idx=1):
        return self.get_element_from_element(self._webtable,
                                             (By.XPATH, f"//tr[{row_idx}]/td[{col_idx}]/input[{input_idx}]"))