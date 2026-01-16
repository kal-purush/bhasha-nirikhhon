window.addEventListener('DOMContentLoaded', function() {
  'use strict';

  // табы
  let tab = document.querySelectorAll('.info-header-tab'),
      info = document.querySelector('.info-header'),
      tabContent = document.querySelectorAll('.info-tabcontent');

  // функция скрытия элементов
  function hideTabContent(a) {
    for (let i = a; i < tabContent.length; i++) {
      tabContent[i].classList.remove('show');
      tabContent[i].classList.add('hide');
    }
  }
  // скрываем все эелменты, кроме первого
  hideTabContent(1);

  // показать элементы
  function showTabContent(b) {
    if (tabContent[b].classList.contains('hide')) {
      tabContent[b].classList.remove('hide');
      tabContent[b].classList.add('show');
    }
  }

  // при клике делаем переключение контента
  info.addEventListener('click', function(event) {
    let target = event.target;
    // проверяем, что то, на что мы кликнули вообще есть и имеет нужный нам класс
    if (target && target.classList.contains('info-header-tab')) {
      for (let i = 0; i < tab.length; i++) {
        // находим таб,на который мы кликнули и скрываем все элементы контента, а показываем только с нужным индексом.
        if (target == tab[i]) {
          hideTabContent(0);
          showTabContent(i);
          break;
        }
      }
    }
  });
});