def generate():                  # Терпит неудачу при выполнении под управлением
    class Spam:                  # Python до версии 2.2
        count = 1

        def method(self):        # Имя Spam недоступно:
            print(Spam.count)    # Не локальное (def), не глобальное (модуль),
                                 # не встроенное
        return Spam()

generate().method()