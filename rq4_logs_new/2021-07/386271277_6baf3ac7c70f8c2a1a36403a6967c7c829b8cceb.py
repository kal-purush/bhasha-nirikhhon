import webbrowser
from win10toast_click import ToastNotifier
import time

hours = [12, 23]
minute = 0
urls = ["https://mail.google.com/mail/u/0/#inbox",
        "https://webmail.trakya.edu.tr/webmail2/#mailbox/INBOX.Sent/msg4"]
icons = ["../images/gmail_icon.ico", "../images/trakya_mail.ico"]
while True:
    time.sleep(60)
    while True:
        all_time = time.localtime(time.time())
        for hour in hours:
            if hour == all_time.tm_hour and minute == all_time.tm_min:

                # functions//Fonksiyonlar
                counter = 0
                for url in urls:
                    def open_url():
                        webbrowser.open_new(url)

                    toaster = ToastNotifier()

                    toaster.show_toast(
                        "Mail Hatırlatması!",  # Title/Başlık
                        "Mail adresine yönlendirilmek için tıklayın.",  # Message/Mesaj
                        icon_path=icons[counter],
                        # Bildirimlerin ne kadar süre açık kalacağını gösterir./ The timer of notification.
                        duration=1,
                        callback_on_click=open_url,
                    )
                    counter += 1
        break
if __name__ == '__main__':
    pass