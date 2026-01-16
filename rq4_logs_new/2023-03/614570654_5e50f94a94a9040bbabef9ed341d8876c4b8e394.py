import time


def get_lists_of_smartphones_and_prices(page, model_locator, price_locator, models_tecno_list=[],
                                       models_techno_price_list=[]):
    model_name_list = page.locator(model_locator)
    model_price_list = page.locator(price_locator)
    count_models = model_name_list.count()
    count_prices = model_price_list.count()
    for i in range(count_prices):
        price = model_price_list.nth(i).get_attribute("title")
        if price is None:
            break
        model = model_name_list.nth(i).text_content()
        if "больше" not in model:
           models_tecno_list.append(model.strip().split("Смартфон ")[1].rsplit(" (", maxsplit=1)[0])
           models_techno_price_list.append(price.strip().split("Цена: ")[1])
    return models_tecno_list, models_techno_price_list


def get_quantity_of_pagination_pages(page, locator):
    return page.locator(locator).count()


def wait_func(t=1):
    time.sleep(t)


def create_csv_file_with_chosen_models(lists_of_models_and_prices):
    with open("Tecno.csv", "w", encoding="UTF-8") as tecno:
        tecno.write("№, Model, Price\n")
        for ind, el in enumerate(lists_of_models_and_prices[0]):
            tecno.write(f"{ind+1}, {el}, {lists_of_models_and_prices[1][ind]}\n")