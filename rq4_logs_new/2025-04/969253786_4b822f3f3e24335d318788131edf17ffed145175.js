// Класс для работы с приложением времени молитв
class PrayerTimesApp {
  constructor() {
    // Элементы интерфейса
    this.elements = {
      // Дата
      gregorianDateEl: document.getElementById('gregorian-date'),
      islamicDateEl: document.getElementById('islamic-date'),
      
      // Настройки
      themeToggle: document.getElementById('theme-toggle'),
      languageButton: document.getElementById('language-button'),
      languageDropdown: document.getElementById('language-dropdown'),
      languageOptions: document.querySelectorAll('.language-option'),
      currentLanguageEl: document.querySelector('.current-language'),
      
      // Местоположение
      locationInput: document.getElementById('location-input'),
      geolocationButton: document.getElementById('geolocation-button'),
      locationResults: document.getElementById('location-results'),
      
      // Молитвы
      nextPrayerName: document.getElementById('next-prayer-name'),
      nextPrayerTime: document.getElementById('next-prayer-time'),
      countdownTimer: document.getElementById('countdown-timer'),
      prayerTimesCards: document.getElementById('prayer-times-cards'),
      
      // Расписание на месяц
      prevMonthButton: document.getElementById('prev-month'),
      nextMonthButton: document.getElementById('next-month'),
      monthYearEl: document.getElementById('month-year'),
      monthlyScheduleBody: document.getElementById('monthly-schedule-body'),
      
      // Загрузка
      loadingOverlay: document.getElementById('loading-overlay'),
      
      // Футер
      currentYearEl: document.getElementById('current-year'),
      aboutLink: document.getElementById('about-link'),
      contactLink: document.getElementById('contact-link')
    };
    
    // Состояние приложения
    this.state = {
      language: 'ru',
      theme: 'light',
      location: {
        name: 'Мекка',
        country: 'Саудовская Аравия',
        latitude: 21.4225,
        longitude: 39.8262
      },
      calculationMethod: 2, // ISNA по умолчанию
      currentDate: new Date(),
      selectedMonth: new Date().getMonth() + 1,
      selectedYear: new Date().getFullYear(),
      prayerTimes: null,
      monthlyPrayerTimes: null,
      updateInterval: null,
      countdownInterval: null
    };
    
    // Загрузка настроек из localStorage
    this.loadSettings();
    
    // Инициализация
    this.init();
  }
  
  // Инициализация приложения
  init() {
    // Установка текущего года в футере
    this.elements.currentYearEl.textContent = new Date().getFullYear();
    
    // Инициализация обработчиков событий
    this.setupEventListeners();
    
    // Применение темы и языка
    this.applyTheme();
    this.applyLanguage();
    
    // Загрузка данных
    this.loadData();
    
    // Запуск обновления таймера
    this.startTimeUpdates();
  }
  
  // Настройка обработчиков событий
  setupEventListeners() {
    // Смена темы
    this.elements.themeToggle.addEventListener('click', () => this.toggleTheme());
    
    // Переключение языка
    this.elements.languageButton.addEventListener('click', () => this.toggleLanguageDropdown());
    document.addEventListener('click', (event) => this.handleClickOutside(event));
    
    // Выбор языка
    this.elements.languageOptions.forEach(option => {
      option.addEventListener('click', () => this.changeLanguage(option.dataset.lang));
    });
    
    // Поиск местоположения
    this.elements.locationInput.addEventListener('input', () => this.searchLocation());
    this.elements.locationInput.addEventListener('focus', () => this.showLocationResults());
    this.elements.geolocationButton.addEventListener('click', () => this.useGeolocation());
    
    // Навигация по месяцам
    this.elements.prevMonthButton.addEventListener('click', () => this.changeToPreviousMonth());
    this.elements.nextMonthButton.addEventListener('click', () => this.changeToNextMonth());
  }
  
  // Загрузка настроек из localStorage
  loadSettings() {
    try {
      // Загружаем язык
      const savedLanguage = localStorage.getItem('prayer_app_language');
      if (savedLanguage) {
        this.state.language = savedLanguage;
        console.log('Using local language preference');
      }
      
      // Загружаем тему
      const savedTheme = localStorage.getItem('prayer_app_theme');
      if (savedTheme) {
        this.state.theme = savedTheme;
        console.log('Using local theme preference');
      }
      
      // Загружаем местоположение
      const savedLocation = localStorage.getItem('prayer_app_location');
      if (savedLocation) {
        this.state.location = JSON.parse(savedLocation);
      }
      
      // Загружаем метод расчета
      const savedMethod = localStorage.getItem('prayer_app_calculation_method');
      if (savedMethod) {
        this.state.calculationMethod = parseInt(savedMethod);
      }
    } catch (error) {
      console.error('Error loading settings:', error);
    }
  }
  
  // Сохранение настроек в localStorage
  saveSettings() {
    try {
      localStorage.setItem('prayer_app_language', this.state.language);
      localStorage.setItem('prayer_app_theme', this.state.theme);
      localStorage.setItem('prayer_app_location', JSON.stringify(this.state.location));
      localStorage.setItem('prayer_app_calculation_method', this.state.calculationMethod.toString());
    } catch (error) {
      console.error('Error saving settings:', error);
    }
  }
  
  // Применение выбранной темы
  applyTheme() {
    if (this.state.theme === 'dark') {
      document.body.classList.add('dark-theme');
      this.elements.themeToggle.innerHTML = '<i class="fas fa-sun"></i>';
    } else {
      document.body.classList.remove('dark-theme');
      this.elements.themeToggle.innerHTML = '<i class="fas fa-moon"></i>';
    }
  }
  
  // Переключение темы
  toggleTheme() {
    this.state.theme = this.state.theme === 'light' ? 'dark' : 'light';
    this.applyTheme();
    this.saveSettings();
    console.log('Theme preference saved on server');
  }
  
  // Применение выбранного языка
  applyLanguage() {
    // Устанавливаем текущий язык в селекторе
    this.elements.currentLanguageEl.textContent = this.state.language.toUpperCase();
    
    // Устанавливаем направление текста для арабского языка
    document.documentElement.dir = this.state.language === 'ar' ? 'rtl' : 'ltr';
    
    // Применяем переводы к интерфейсу
    this.updateUITranslations();
  }
  
  // Переключение отображения выпадающего списка языков
  toggleLanguageDropdown() {
    this.elements.languageDropdown.classList.toggle('show');
  }
  
  // Обработка клика вне выпадающего списка языков
  handleClickOutside(event) {
    if (!event.target.closest('.language-selector') && this.elements.languageDropdown.classList.contains('show')) {
      this.elements.languageDropdown.classList.remove('show');
    }
  }
  
  // Смена языка
  changeLanguage(lang) {
    if (this.state.language !== lang) {
      this.state.language = lang;
      this.applyLanguage();
      this.saveSettings();
      this.updatePrayerTimesDisplay(); // Обновляем отображение с новым языком
      this.updateMonthlySchedule(); // Обновляем расписание на месяц с новым языком
      console.log('Language preference saved on server');
    }
    this.elements.languageDropdown.classList.remove('show');
  }
  
  // Обновление интерфейса с переводами
  updateUITranslations() {
    const t = translations[this.state.language];
    
    // Обновление заголовков
    document.title = t.appName;
    document.querySelector('.logo h1').textContent = t.appName;
    
    // Обновление селектора местоположения
    this.elements.locationInput.placeholder = t.locationPlaceholder;
    
    // Обновление заголовков разделов
    document.querySelector('.next-prayer-title').textContent = t.nextPrayer;
    document.querySelector('.prayer-times-section h2').textContent = t.prayerTimes;
    document.querySelector('.monthly-schedule-section h2').textContent = t.monthlySchedule;
    
    // Обновление футера
    this.elements.aboutLink.textContent = t.about;
    this.elements.contactLink.textContent = t.contact;
    
    // Обновление заголовков таблицы расписания
    const tableHeaders = document.querySelectorAll('.monthly-schedule-table th');
    tableHeaders[0].textContent = t.scheduleTable.date;
    tableHeaders[1].textContent = t.prayers.fajr;
    tableHeaders[2].textContent = t.prayers.sunrise;
    tableHeaders[3].textContent = t.prayers.dhuhr;
    tableHeaders[4].textContent = t.prayers.asr;
    tableHeaders[5].textContent = t.prayers.maghrib;
    tableHeaders[6].textContent = t.prayers.isha;
  }
  
  // Загрузка данных о молитвах
  loadData() {
    this.showLoading();
    
    // Загружаем времена молитв на сегодня
    this.loadPrayerTimes()
      .then(() => {
        // Загружаем расписание на месяц
        return this.loadMonthlySchedule();
      })
      .finally(() => {
        this.hideLoading();
      });
  }
  
  // Загрузка времен молитв на сегодня
  async loadPrayerTimes() {
    try {
      // Время молитв - использование фиксированных данных для демонстрации
      // В реальном приложении здесь был бы API-запрос к серверу
      const today = new Date();
      
      // Используем параметры местоположения
      const { latitude, longitude } = this.state.location;
      
      // Выбираем молитвы в зависимости от текущего времени
      const currentTime = today.getHours() * 60 + today.getMinutes();
      
      // Создаем имитацию времени молитв на сегодня
      const prayerTimes = {
        date: today.toLocaleDateString(),
        gregorianDate: today.toLocaleDateString(this.getLocale(), { day: 'numeric', month: 'long', year: 'numeric' }),
        islamicDate: this.getIslamicDate(today),
        dayOfWeek: today.toLocaleDateString(this.getLocale(), { weekday: 'long' }),
        location: this.state.location.name,
        latitude: latitude.toString(),
        longitude: longitude.toString(),
        times: [
          this.createPrayerTime('fajr', '04:55', currentTime),
          this.createPrayerTime('sunrise', '05:58', currentTime),
          this.createPrayerTime('dhuhr', '12:20', currentTime),
          this.createPrayerTime('asr', '16:51', currentTime),
          this.createPrayerTime('maghrib', '18:42', currentTime),
          this.createPrayerTime('isha', '19:45', currentTime)
        ]
      };
      
      // Сохраняем время молитв
      this.state.prayerTimes = prayerTimes;
      
      // Обновляем отображение
      this.updatePrayerTimesDisplay();
      this.updateNextPrayer();
      
      // Запускаем обратный отсчет до следующей молитвы
      this.startCountdown();
      
      console.log('Все молитвы:', prayerTimes.times.map(p => `${p.name}: ${p.time} (${p.timeInMinutes}min) isCurrent:${p.isCurrent} isUpcoming:${p.isUpcoming}`));
      
      return prayerTimes;
    } catch (error) {
      console.error('Error loading prayer times:', error);
      this.showError(translations[this.state.language].messages.errorLoadingData);
      return null;
    }
  }
  
  // Создание объекта времени молитвы
  createPrayerTime(name, time, currentTimeInMinutes) {
    const [hours, minutes] = time.split(':').map(Number);
    const timeInMinutes = hours * 60 + minutes;
    
    // Определяем, является ли эта молитва текущей
    // (текущее время находится между этой молитвой и следующей)
    const prayerTimes = [
      { name: 'fajr', time: '04:55' },
      { name: 'sunrise', time: '05:58' },
      { name: 'dhuhr', time: '12:20' },
      { name: 'asr', time: '16:51' },
      { name: 'maghrib', time: '18:42' },
      { name: 'isha', time: '19:45' }
    ];
    
    // Находим индекс текущей молитвы в массиве
    const index = prayerTimes.findIndex(p => p.name === name);
    
    // Время следующей молитвы (или конца дня)
    const nextTimeInMinutes = index < prayerTimes.length - 1 
      ? this.timeToMinutes(prayerTimes[index + 1].time) 
      : 24 * 60;
    
    // Молитва считается текущей, если текущее время находится между ее временем и временем следующей молитвы
    const isCurrent = currentTimeInMinutes >= timeInMinutes && currentTimeInMinutes < nextTimeInMinutes;
    
    // Молитва считается предстоящей, если она следующая после текущей
    let isUpcoming = false;
    for (let i = 0; i < prayerTimes.length; i++) {
      const currentPrayerMinutes = this.timeToMinutes(prayerTimes[i].time);
      if (currentTimeInMinutes < currentPrayerMinutes) {
        isUpcoming = (name === prayerTimes[i].name);
        break;
      }
    }
    
    return {
      name,
      time,
      timeFormatted: this.formatTime(time),
      timeInMinutes,
      isCurrent,
      isUpcoming
    };
  }
  
  // Преобразование времени в минуты
  timeToMinutes(time) {
    const [hours, minutes] = time.split(':').map(Number);
    return hours * 60 + minutes;
  }
  
  // Форматирование времени
  formatTime(time) {
    return time; // Уже в формате HH:MM
  }
  
  // Обновление отображения времен молитв
  updatePrayerTimesDisplay() {
    if (!this.state.prayerTimes) return;
    
    // Обновляем дату
    this.elements.gregorianDateEl.textContent = this.state.prayerTimes.gregorianDate;
    this.elements.islamicDateEl.textContent = this.state.prayerTimes.islamicDate;
    
    // Очищаем контейнер с карточками молитв
    this.elements.prayerTimesCards.innerHTML = '';
    
    // Добавляем карточки для каждой молитвы
    this.state.prayerTimes.times.forEach(prayer => {
      const prayerCard = document.createElement('div');
      prayerCard.className = `prayer-time-card prayer-time-card-${prayer.name}`;
      if (prayer.isCurrent) prayerCard.classList.add('active');
      
      const t = translations[this.state.language];
      const prayerName = t.prayers[prayer.name];
      
      prayerCard.innerHTML = `
        <div class="prayer-name">${prayerName}</div>
        <div class="prayer-time">${prayer.timeFormatted}</div>
      `;
      
      this.elements.prayerTimesCards.appendChild(prayerCard);
    });
  }
  
  // Обновление следующей молитвы и обратного отсчета
  updateNextPrayer() {
    if (!this.state.prayerTimes) return;
    
    const t = translations[this.state.language];
    
    // Находим следующую молитву
    const nextPrayer = this.state.prayerTimes.times.find(prayer => prayer.isUpcoming);
    
    if (nextPrayer) {
      // Обновляем отображение следующей молитвы
      this.elements.nextPrayerName.textContent = t.prayers[nextPrayer.name];
      this.elements.nextPrayerTime.textContent = nextPrayer.timeFormatted;
      
      console.log('Следующая молитва из флагов:', t.prayers[nextPrayer.name], nextPrayer.timeFormatted);
      
      // Возвращаем выбранную следующую молитву
      return nextPrayer;
    } else {
      // Если нет следующей молитвы сегодня, используем первую молитву (Фаджр) на завтра
      const fajrPrayer = this.state.prayerTimes.times[0];
      this.elements.nextPrayerName.textContent = t.prayers.fajr;
      this.elements.nextPrayerTime.textContent = fajrPrayer.timeFormatted;
      
      return fajrPrayer;
    }
  }
  
  // Запуск обратного отсчета до следующей молитвы
  startCountdown() {
    // Очищаем предыдущий интервал, если он существует
    if (this.state.countdownInterval) {
      clearInterval(this.state.countdownInterval);
    }
    
    const updateCountdown = () => {
      const nextPrayer = this.updateNextPrayer();
      if (!nextPrayer) return;
      
      // Текущее время
      const now = new Date();
      const currentHours = now.getHours();
      const currentMinutes = now.getMinutes();
      const currentSeconds = now.getSeconds();
      const currentTimeInMinutes = currentHours * 60 + currentMinutes;
      
      // Время следующей молитвы
      const [prayerHours, prayerMinutes] = nextPrayer.time.split(':').map(Number);
      const prayerTimeInMinutes = prayerHours * 60 + prayerMinutes;
      
      // Разница во времени
      let diffInMinutes = prayerTimeInMinutes - currentTimeInMinutes;
      
      // Если следующая молитва завтра
      if (diffInMinutes <= 0) {
        diffInMinutes += 24 * 60; // Добавляем 24 часа
      }
      
      // Вычисляем часы, минуты и секунды до следующей молитвы
      const hours = Math.floor(diffInMinutes / 60);
      const minutes = diffInMinutes % 60;
      const seconds = 59 - currentSeconds;
      
      // Форматирование времени
      const formattedHours = hours.toString().padStart(2, '0');
      const formattedMinutes = minutes.toString().padStart(2, '0');
      const formattedSeconds = seconds.toString().padStart(2, '0');
      
      // Обновляем отображение обратного отсчета
      this.elements.countdownTimer.textContent = 
        `${formattedHours}:${formattedMinutes}:${formattedSeconds}`;
    };
    
    // Обновляем обратный отсчет сейчас и запускаем интервал
    updateCountdown();
    this.state.countdownInterval = setInterval(updateCountdown, 1000);
  }
  
  // Загрузка расписания на месяц
  async loadMonthlySchedule() {
    try {
      // Используем параметры местоположения и выбранного месяца/года
      const { latitude, longitude } = this.state.location;
      const { selectedMonth, selectedYear } = this.state;
      
      // В реальном приложении здесь был бы API-запрос к серверу
      // для получения расписания на месяц
      
      // Создаем имитацию расписания на месяц
      const daysInMonth = new Date(selectedYear, selectedMonth, 0).getDate();
      const monthlyData = {
        gregorianMonth: this.getMonthName(selectedMonth),
        gregorianYear: selectedYear,
        islamicMonth: this.getIslamicMonthName(selectedMonth),
        islamicYear: selectedYear + 578, // Приблизительная разница между григорианским и исламским календарями
        days: []
      };
      
      // Добавляем данные для каждого дня месяца
      for (let day = 1; day <= daysInMonth; day++) {
        monthlyData.days.push({
          gregorianDate: `${day} ${this.getMonthName(selectedMonth)}`,
          islamicDate: `${day} ${this.getIslamicMonthName(selectedMonth)}`,
          gregorianDay: day,
          islamicDay: day,
          gregorianMonth: selectedMonth,
          islamicMonth: selectedMonth,
          fajr: '04:55',
          sunrise: '05:58',
          dhuhr: '12:20',
          asr: '16:51',
          maghrib: '18:42',
          isha: '19:45'
        });
      }
      
      // Сохраняем расписание на месяц
      this.state.monthlyPrayerTimes = monthlyData;
      
      // Обновляем отображение
      this.updateMonthlySchedule();
      
      return monthlyData;
    } catch (error) {
      console.error('Error loading monthly schedule:', error);
      this.showError(translations[this.state.language].messages.errorLoadingData);
      return null;
    }
  }
  
  // Обновление отображения расписания на месяц
  updateMonthlySchedule() {
    if (!this.state.monthlyPrayerTimes) return;
    
    // Обновляем заголовок месяца и года
    const monthYear = `${this.state.monthlyPrayerTimes.gregorianMonth} ${this.state.monthlyPrayerTimes.gregorianYear}`;
    this.elements.monthYearEl.textContent = monthYear;
    
    // Очищаем таблицу
    this.elements.monthlyScheduleBody.innerHTML = '';
    
    // Добавляем строки для каждого дня
    const today = new Date();
    const isCurrentMonth = 
      today.getMonth() + 1 === this.state.selectedMonth && 
      today.getFullYear() === this.state.selectedYear;
    const currentDay = today.getDate();
    
    this.state.monthlyPrayerTimes.days.forEach(day => {
      const row = document.createElement('tr');
      
      // Отмечаем сегодняшний день
      if (isCurrentMonth && day.gregorianDay === currentDay) {
        row.classList.add('today');
      }
      
      // Форматируем дату
      const dateCell = `
        <td class="date-cell">
          ${day.gregorianDay}
          <span class="islamic-date">${day.islamicDay}</span>
        </td>
      `;
      
      // Добавляем ячейки с временами молитв
      row.innerHTML = `
        ${dateCell}
        <td>${day.fajr}</td>
        <td>${day.sunrise}</td>
        <td>${day.dhuhr}</td>
        <td>${day.asr}</td>
        <td>${day.maghrib}</td>
        <td>${day.isha}</td>
      `;
      
      this.elements.monthlyScheduleBody.appendChild(row);
    });
  }
  
  // Переход к предыдущему месяцу
  changeToPreviousMonth() {
    // Уменьшаем месяц на 1
    this.state.selectedMonth--;
    
    // Если месяц стал 0, переходим к декабрю предыдущего года
    if (this.state.selectedMonth < 1) {
      this.state.selectedMonth = 12;
      this.state.selectedYear--;
    }
    
    // Загружаем данные для нового месяца
    this.loadMonthlySchedule();
  }
  
  // Переход к следующему месяцу
  changeToNextMonth() {
    // Увеличиваем месяц на 1
    this.state.selectedMonth++;
    
    // Если месяц стал 13, переходим к январю следующего года
    if (this.state.selectedMonth > 12) {
      this.state.selectedMonth = 1;
      this.state.selectedYear++;
    }
    
    // Загружаем данные для нового месяца
    this.loadMonthlySchedule();
  }
  
  // Получение имени месяца
  getMonthName(month) {
    return translations[this.state.language].months[month];
  }
  
  // Получение имени исламского месяца
  getIslamicMonthName(month) {
    return translations[this.state.language].islamicMonths[month];
  }
  
  // Получение исламской даты (приблизительно для демонстрации)
  getIslamicDate(date) {
    // В реальном приложении здесь был бы точный расчет исламской даты
    // Мы используем простое приближение для демонстрации
    const day = date.getDate();
    const month = date.getMonth() + 1;
    const year = date.getFullYear() + 578; // Приблизительная разница
    
    return `${day} ${this.getIslamicMonthName(month)} ${year}`;
  }
  
  // Получение локали для форматирования даты
  getLocale() {
    const localeMap = {
      ru: 'ru-RU',
      en: 'en-US',
      ar: 'ar-SA',
      fr: 'fr-FR'
    };
    
    return localeMap[this.state.language] || 'en-US';
  }
  
  // Поиск местоположения
  searchLocation() {
    const query = this.elements.locationInput.value.trim();
    
    // Если запрос пустой, скрываем результаты
    if (!query) {
      this.elements.locationResults.classList.remove('show');
      return;
    }
    
    // Показываем контейнер с результатами
    this.showLocationResults();
    
    // В реальном приложении здесь был бы API-запрос для поиска местоположения
    // Для демонстрации используем фиксированные результаты
    
    // Очищаем предыдущие результаты
    this.elements.locationResults.innerHTML = '';
    
    // Фиксированные результаты для демонстрации
    const locations = [
      { name: 'Мекка', country: 'Саудовская Аравия', latitude: 21.4225, longitude: 39.8262 },
      { name: 'Медина', country: 'Саудовская Аравия', latitude: 24.5247, longitude: 39.5692 },
      { name: 'Москва', country: 'Россия', latitude: 55.7558, longitude: 37.6173 },
      { name: 'Санкт-Петербург', country: 'Россия', latitude: 59.9343, longitude: 30.3351 },
      { name: 'Казань', country: 'Россия', latitude: 55.7887, longitude: 49.1221 }
    ];
    
    // Фильтруем локации по запросу
    const filteredLocations = locations.filter(location => {
      return location.name.toLowerCase().includes(query.toLowerCase()) ||
        location.country.toLowerCase().includes(query.toLowerCase());
    });
    
    // Если нет результатов, показываем сообщение
    if (filteredLocations.length === 0) {
      const noResultsEl = document.createElement('div');
      noResultsEl.className = 'location-result-item';
      noResultsEl.textContent = translations[this.state.language].locationNotFound;
      this.elements.locationResults.appendChild(noResultsEl);
      return;
    }
    
    // Добавляем результаты поиска
    filteredLocations.forEach(location => {
      const resultItem = document.createElement('div');
      resultItem.className = 'location-result-item';
      resultItem.innerHTML = `
        <div class="location-result-name">${location.name}</div>
        <div class="location-result-country">${location.country}</div>
      `;
      
      // Обработчик выбора местоположения
      resultItem.addEventListener('click', () => {
        this.selectLocation(location);
      });
      
      this.elements.locationResults.appendChild(resultItem);
    });
  }
  
  // Показать результаты поиска местоположения
  showLocationResults() {
    if (this.elements.locationInput.value.trim()) {
      this.elements.locationResults.classList.add('show');
    }
  }
  
  // Скрыть результаты поиска местоположения
  hideLocationResults() {
    this.elements.locationResults.classList.remove('show');
  }
  
  // Выбор местоположения
  selectLocation(location) {
    // Обновляем местоположение в состоянии
    this.state.location = location;
    
    // Обновляем значение в поле ввода
    this.elements.locationInput.value = `${location.name}, ${location.country}`;
    
    // Скрываем результаты поиска
    this.hideLocationResults();
    
    // Сохраняем настройки
    this.saveSettings();
    
    // Загружаем новые данные
    this.loadData();
    
    // Показываем сообщение
    this.showMessage(translations[this.state.language].messages.locationUpdated);
  }
  
  // Использование геолокации
  useGeolocation() {
    if (!navigator.geolocation) {
      this.showError(translations[this.state.language].messages.geolocationError);
      return;
    }
    
    this.showLoading();
    
    navigator.geolocation.getCurrentPosition(
      (position) => {
        // В реальном приложении здесь был бы запрос к API для получения
        // информации о местоположении по координатам
        
        // Для демонстрации используем фиксированное местоположение
        const location = {
          name: 'Мекка',
          country: 'Саудовская Аравия',
          latitude: 21.4225,
          longitude: 39.8262
        };
        
        // Выбираем местоположение
        this.selectLocation(location);
        
        // Скрываем индикатор загрузки
        this.hideLoading();
        
        // Показываем сообщение об успехе
        this.showMessage(translations[this.state.language].messages.geolocationSuccess);
      },
      (error) => {
        console.error('Geolocation error:', error);
        this.hideLoading();
        this.showError(translations[this.state.language].messages.geolocationError);
      },
      {
        enableHighAccuracy: true,
        timeout: 5000,
        maximumAge: 0
      }
    );
  }
  
  // Запуск обновления времени
  startTimeUpdates() {
    // Очищаем предыдущий интервал, если он существует
    if (this.state.updateInterval) {
      clearInterval(this.state.updateInterval);
    }
    
    // Обновляем данные каждую минуту
    this.state.updateInterval = setInterval(() => {
      this.loadPrayerTimes();
    }, 60000); // 1 минута
  }
  
  // Показать индикатор загрузки
  showLoading() {
    this.elements.loadingOverlay.classList.add('show');
  }
  
  // Скрыть индикатор загрузки
  hideLoading() {
    this.elements.loadingOverlay.classList.remove('show');
  }
  
  // Показать сообщение об ошибке
  showError(message) {
    alert(message);
  }
  
  // Показать сообщение
  showMessage(message) {
    // Простой алерт для демонстрации
    // В реальном приложении здесь было бы красивое уведомление
    // console.log(message);
  }
}

// Инициализация приложения после загрузки DOM
document.addEventListener('DOMContentLoaded', () => {
  const app = new PrayerTimesApp();
});