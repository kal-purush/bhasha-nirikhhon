require('jquery-ui/ui/widgets/datepicker.js');


const $filterElement = $('.ui-input__field_for-datepicker-range');
const $filterElementHide = $('.ui-input__field_for-hide-datepicker');

const clearInput = () => {
  $filterElement.val('');
};

const applyDatepicker = (evt) => {
  evt.data.value.datepicker('hide');
};

$filterElement.datepicker({
  showOtherMonths: true,
  dayNamesMin: ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб'],
  dateFormat: 'dd M',
  monthNames: ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль',
    'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'],
  monthNamesShort: [' янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'],
  nextText: '',
  firstDay: 1,
  showButtonPanel: true,
  closeText: 'очистить',
  currentText: 'применить',
  beforeShow: (text, instance) => {
    setTimeout(() => {
      instance.dpDiv.css({
        top: $filterElement.offset().top + 44,
        left: $filterElement.offset().left,
        width: $filterElement.outerWidth(),
      });

      const $clearButton = $('.ui-datepicker-close');
      const $applyButton = $('.ui-datepicker-current');

      $clearButton.click(clearInput);
      $applyButton.click({ value: $filterElement }, applyDatepicker);
    }, 100);
  },
  onClose: (value) => {
    if (!value) {
      $filterElement.datepicker('setDate', null);
    } else $filterElementHide.datepicker('show');
  },
});

$filterElementHide.datepicker({
  showOtherMonths: true,
  dayNamesMin: ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб'],
  dateFormat: 'dd M',
  monthNames: ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль',
    'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'],
  monthNamesShort: [' янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'],
  nextText: '',
  firstDay: 1,
  showButtonPanel: true,
  closeText: 'очистить',
  currentText: 'применить',
  onClose: (value) => {
    if (value === '') {
      $filterElementHide.datepicker('setDate', null);
    } else {
      $filterElement.val(`${$filterElement.val().substring(0, 6)} - ${value}`);
    }
  },
  beforeShow: (text, instance) => {
    setTimeout(() => {
      instance.dpDiv.css({
        top: $filterElement.offset().top + 44,
        left: $filterElement.offset().left,
      });

      const $clearButton = $('.ui-datepicker-close');
      const $applyButton = $('.ui-datepicker-current');

      $clearButton.click(clearInput);
      $applyButton.click({ value: $filterElementHide }, applyDatepicker);
    }, 100);
  },
});