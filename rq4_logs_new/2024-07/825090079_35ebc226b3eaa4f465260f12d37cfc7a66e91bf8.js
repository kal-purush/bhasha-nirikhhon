'use strict';

$(document).ready(() => {
    //<ПЕРЕМЕННЫЕ>==========================================================================
    const elements = {
        headerBurger: $('.header__burger'),
        headerContent: $('.header__content'),
        body: $('body'),
        anchors: $('a[href*="#"]'),
        mainButton: $('.main__button'),
        products: $('#products'),
        productButton: $('.product-item__button'),
        order: $('#order'),
        loader: $('.loader'),
        buttonSubmit: $('.order-form__button'),
        orderProduct: $("input[name*='product']"),
        orderName: $("input[name*='name']"),
        orderPhone: $("input[name*='phone']"),
        form: $('.order-form'),
        orderTitle: $('.order__title'),
        orderText: $('.order__text'),
        orderBody: $('.order__body'),
        orderInfo: $('.order__info'),
        warnings: $('.order-form__warning-message'),
        date: $('#date')
    };

    //<ФУНКЦИЯ ПЛАВНОГО ПЕРЕХОДА>=============================================================
    const scrollTo = (target) => {
        target.scrollIntoView({
            behavior: "smooth",
            block: "start"
        });
    };

    //<ФУНКЦИИ ПО РАБОТЕ С ФОРМОЙ>=============================================================
    const validate = (input) => {
        let error = false;
        if (!input.val()) {
            input.css('border', '3px solid rgb(255, 0, 0)').next().show();
            error = true;
        } else {
            input.css('border', '3px solid #097f09').next().hide();
        }
        return error;
    };

    //<МЕНЮ БУРГЕР>=========================================================================
    elements.headerBurger.click(() => {
        elements.headerBurger.toggleClass('active');
        elements.headerContent.toggleClass('active');
        elements.body.toggleClass('lock');
    });

    //<ПЕРЕХОД ПО ССЫЛКАМ-ЯКОРЯМ>============================================================
    elements.anchors.each(function () {
        $(this).click((event) => {
            event.preventDefault();
            const blockID = $(this).attr('href');
            if (elements.body.hasClass('lock')) {
                elements.headerBurger.removeClass('active');
                elements.headerContent.removeClass('active');
                elements.body.removeClass('lock');
            }
            scrollTo($(blockID)[0]);
        });
    });

    //<ПЕРЕХОД ПО КНОПКЕ MAIN>================================================================
    elements.mainButton.click((event) => {
        event.preventDefault();
        scrollTo(elements.products[0]);
    });

    //<ПЕРЕХОД ПО НАЖАТИЮ НА КНОПКУ ТОВАРА И ПОДСТАВЛЕНИЕ ЗНАЧЕНИЕ В ИНПУТ>====================
    elements.productButton.click((event) => {
        event.preventDefault();
        const productNameValue = $(event.target).closest('.product-item').find('.product-item__name').text();
        if (productNameValue) {
            elements.orderProduct.val(productNameValue);
            scrollTo(elements.order[0]);
        }
    });

    //<МАСКА НА ТЕЛЕФОН>==========================================================================
    elements.orderPhone.mask('+375 (00) 000 - 00 - 00');

    //<ОТПРАВКА ДАННЫХ ФОРМЫ>=====================================================================
    elements.buttonSubmit.click((event) => {
        event.preventDefault();
        const inputsToValidate = [elements.orderProduct, elements.orderName, elements.orderPhone];
        inputsToValidate.forEach(validate);
        let hasError = inputsToValidate.some(validate);

        if (!hasError) {
            elements.loader.css('display', 'flex');

            const requestData = {
                product: elements.orderProduct.val(),
                name: elements.orderName.val(),
                phone: elements.orderPhone.val()
            };

            $.ajax({
                method: "POST",
                url: 'https://testologia.ru/checkout',
                data: requestData
            })
                .done((msg) => {
                    elements.loader.hide();
                    if (msg.success) {
                        [elements.form, elements.orderTitle, elements.orderText].forEach(item => item.remove());
                        const successMessage = $("<div class='message-about-success'></div>").text("Спасибо за Ваш заказ. Мы скоро свяжемся с Вами!");
                        elements.orderBody.css("align-items", "center");
                        elements.orderInfo.append(successMessage);
                    } else {
                        alert('Возникла ошибка при оформлении заказа, позвоните нам и сделайте заказ');
                        elements.form[0].reset();
                        inputsToValidate.forEach(item => item.css('border', '1px solid rgb(117, 18, 38)'));
                    }
                });
        }
    });

    // <АКТУАЛЬНЫЙ ГОД>=====================================================================
    elements.date.text(new Date().getFullYear());
});