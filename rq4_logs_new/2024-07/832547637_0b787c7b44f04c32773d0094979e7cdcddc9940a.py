import json
import requests
import xlwt
import os
from datetime import datetime, timedelta
import win32com.client as win32

# URL to query historical exchange rate records
url = "http://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHisNew"

def date_add(time, days):
    """Add a number of days to a datetime object (can be negative)."""
    return time + timedelta(days=days)

def query_records():
    """Query historical exchange rate records for the past 60 days."""
    now = datetime.now()
    end_time = date_add(now, -60)
    data = {
        'startDate': end_time.strftime("%Y-%m-%d"),
        'endDate': now.strftime("%Y-%m-%d"),
        'currency': 'USD/CNY',
        'pageNum': '1',
        'pageSize': '15'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        return result.get('records', [])
    except requests.exceptions.RequestException as e:
        print(f"HTTP Request failed: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON response: {e}")
        return []

def save_to_excel(records, filename="exchange_rates.xls"):
    """Save records to an Excel file."""
    # Create a workbook and add a worksheet
    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet("Exchange Rates")
    
    # Add headers
    headers = ["Date", "USD/CNY", "EUR/CNY", "100JPY/CNY", "HKD/CNY", "GBP/CNY", "AUD/CNY", "NZD/CNY", 
               "SGD/CNY", "CHF/CNY", "CAD/CNY", "CNY/MOP", "CNY/MYR", "CNY/RUB", "CNY/ZAR", "CNY/KRW", 
               "CNY/AED", "CNY/SAR", "CNY/HUF", "CNY/PLN", "CNY/DKK", "CNY/SEK", "CNY/NOK", "CNY/TRY", 
               "CNY/MXN", "CNY/THB"]
    for col, header in enumerate(headers):
        sheet.write(0, col, header)
    
    # Add records
    for row, record in enumerate(records, start=1):
        date = record.get('date', 'N/A')
        values = record.get('values', [])
        
        sheet.write(row, 0, date)  # Write the date
        
        # Write the exchange rates
        for col, value in enumerate(values, start=1):
            sheet.write(row, col, value)
    
    # Save the workbook in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(script_dir, filename)
    workbook.save(filepath)
    return filepath

def send_email(filepath, recipients):
    """Send an email with the Excel file attached using Outlook."""
    try:
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.Subject = "Exchange Rate Records 中国人民银行中间价汇率"
        mail.Body = "附件为近15天的各币种汇率数据"
        mail.Attachments.Add(filepath)
        
        # If recipients is a single email, just assign it directly
        if isinstance(recipients, str):
            mail.To = recipients
        # If recipients is a list of emails, join them with semicolons
        elif isinstance(recipients, list):
            mail.To = "; ".join(recipients)
        
        mail.Send()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Example usage
if __name__ == "__main__":
    records = query_records()
    if records:
        print("Retrieved records:", records)
        excel_path = save_to_excel(records)
        print("Data has been saved to exchange_rates.xls")
        send_email(excel_path, ["recipent1@email.com", "receipent2@email.com"])  # Replace with actual recipient emails
    else:
        print("No records retrieved.")