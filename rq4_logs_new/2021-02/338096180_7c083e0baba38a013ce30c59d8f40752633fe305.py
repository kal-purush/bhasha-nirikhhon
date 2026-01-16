from requests import get, utils
import decimal as d
from datetime import datetime


def currency_rates(currency):
    d.getcontext().prec = 4
    currency = currency.upper()
    response = get("http://www.cbr.ru/scripts/XML_daily.asp")
    encodings = utils.get_encoding_from_headers(response.headers)
    date = " ".join(response.headers["Date"].split(" ")[1:4])
    date_object = datetime.strptime(date, "%d %b %Y").date()
    content = response.content.decode(encoding=encodings)

    if f"<CharCode>{currency}</CharCode>" in content:
        nominal = content[content.find("<Nominal>", content.index(currency)) + 9:content.find("</Nominal>", content.index(currency))]
        value = content[content.find("<Value>", content.index(currency)) + 7:content.find("</Value>", content.index(currency))]
        return str(d.Decimal(value.replace(",", ".")) / d.Decimal(nominal.replace(",", "."))), date_object.strftime("%m/%d/%Y")
    else:
        return None


if __name__ == "__main__":
    print(currency_rates('usd'))
    print(currency_rates('EUR'))