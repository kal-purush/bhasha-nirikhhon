using System;
using System.Collections.Generic;
using System.Linq;

// Календарь исторических событий: http://www.abc-people.com/event/calendar-4.htm

namespace _20180417_WCF_HistoricalEventsServiceLib
{
    internal class HistoricalEventsService : IHistoricalEvents
    {
        #region Common Methods

        /// <inheritdoc />
        /// <summary>
        /// Проверка соединения.
        /// </summary>
        /// <returns>OK</returns>
        public string TestConnection()
        {
            return "OK";
        }

        #endregion

        #region Implementation of the contract

        /// <inheritdoc />
        /// <summary>
        /// Метод InfoAboutEvents() выводит информацию об исторических событиях.
        /// </summary>
        /// <param name="startDate">Дата начала поиска исторических событий.</param>
        /// <returns>Список исторических событий на протяжении недели начиная от стартовой даты.</returns>
        public List<CalendarOfEvents> InfoAboutEvents(DateTime startDate)
        {
            // Cледующие семь дней от текущей даты
            var eventsForTheWeek = from date in events
                                   where date.Date > startDate
                                   where date.Date <= startDate.AddDays(+7)
                                   select date;

            return eventsForTheWeek.ToList();   // Ответ клиенту.
        }

        #endregion

        #region Calendar of historical events.

        private readonly List<CalendarOfEvents> events = new List<CalendarOfEvents>
            {
                new CalendarOfEvents(DateTime.Parse("2018.01.01"), "1788 - Начинается выход английской газеты «Таймс»."),
                new CalendarOfEvents(DateTime.Parse("2018.01.04"), "1493 - Христофор Колумб, открывший Америку, отправляется в обратный путь."),
                new CalendarOfEvents(DateTime.Parse("2018.01.08"), "1873 - В США открыта первая женская тюрьма."),
                new CalendarOfEvents(DateTime.Parse("2018.01.11"), "1909 - Из Нью-Йорка в Филадельфию отправились участницы первой женской автогонки."),
                new CalendarOfEvents(DateTime.Parse("2018.01.15"), "1992 - Президиум Верховной Рады утвердил музыку государственного гимна Украины."),
                new CalendarOfEvents(DateTime.Parse("2018.01.19"), "1978 - В Германии выпущен последний автомобиль «фольксваген-жук»."),
                new CalendarOfEvents(DateTime.Parse("2018.01.24"), "1962 - Брайан Эпштейн становится менеджером группы «Битлз»."),
                new CalendarOfEvents(DateTime.Parse("2018.01.30"), "1894 - В США запатентован пневматический пресс."),
                new CalendarOfEvents(DateTime.Parse("2018.02.02"), "1887 - В США впервые официально праздновали День сурка."),
                new CalendarOfEvents(DateTime.Parse("2018.02.06"), "1935 - Впервые в продажу поступает настольная игра «Монополия»."),
                new CalendarOfEvents(DateTime.Parse("2018.02.11"), "1809 - Роберт Фултон патентует пароход."),
                new CalendarOfEvents(DateTime.Parse("2018.02.16"), "1979 - Джордж Харрисон выпустил альбом Blow Away."),
                new CalendarOfEvents(DateTime.Parse("2018.02.22"), "1819 - Испанией Флорида была продана за 5 млн $ США."),
                new CalendarOfEvents(DateTime.Parse("2018.02.28"), "1936 - Гитлер открывает зимние Олимпийский игры."),
                new CalendarOfEvents(DateTime.Parse("2018.03.04"), "2006 - Открыта станция Киевского метрополитена «Вырлица»."),
                new CalendarOfEvents(DateTime.Parse("2018.03.08"), "1887 - Эверетт Хортон запатентовал телескопическую удочку."),
                new CalendarOfEvents(DateTime.Parse("2018.03.14"), "1994 - Релиз Linux версии 1.0.0."),
                new CalendarOfEvents(DateTime.Parse("2018.03.20"), "1613 - День подвига Сусанина."),
                new CalendarOfEvents(DateTime.Parse("2018.03.25"), "2002 - Обнаружен ген эпилепсии."),
                new CalendarOfEvents(DateTime.Parse("2018.03.30"), "1954 - В Торонто открыта первая в Канаде линия метро."),
                new CalendarOfEvents(DateTime.Parse("2018.04.04"), "1850 - Лос-Анджелес стал городом."),
                new CalendarOfEvents(DateTime.Parse("2018.04.08"), "1766 - В США запатентована первая пожарная лестница."),
                new CalendarOfEvents(DateTime.Parse("2018.04.12"), "1995 - Запущен каталог Yahoo!, быстро ставший одним из самых популярных в мире."),
                new CalendarOfEvents(DateTime.Parse("2018.04.17"), "1923 - XII съезд РКП."),
                new CalendarOfEvents(DateTime.Parse("2018.04.18"), "1909 - Ватикан канонизировал Жанну ДАрк."),
                new CalendarOfEvents(DateTime.Parse("2018.04.20"), "1917 - Первый полет первого дирижабля ВМС США DN-1."),
                new CalendarOfEvents(DateTime.Parse("2018.04.21"), "1979 - В Киеве открылся театр драмы и комедии."),
                new CalendarOfEvents(DateTime.Parse("2018.04.23"), "1959 - Создан Союз журналистов Украины."),
                new CalendarOfEvents(DateTime.Parse("2018.04.24"), "1968 - Маврикий принят в ООН."),
                new CalendarOfEvents(DateTime.Parse("2018.04.26"), "1986 - Авария на Чернобыльской АЭС."),
                new CalendarOfEvents(DateTime.Parse("2018.04.28"), "1914 - В США запатентован воздушный кондиционер."),
                new CalendarOfEvents(DateTime.Parse("2018.04.30"), "1934 - Принята новая Конституции Австрии."),
                new CalendarOfEvents(DateTime.Parse("2018.05.01"), "1006 - Вспышка сверхновая звезды SN 1006."),
                new CalendarOfEvents(DateTime.Parse("2018.05.04"), "1976 - Первый концерт группы «Kiss»."),
                new CalendarOfEvents(DateTime.Parse("2018.05.06"), "1908 - Братья Райт возобновили полёты после трёхлетнего перерыва."),
                new CalendarOfEvents(DateTime.Parse("2018.05.09"), "1955 - ФРГ принята в НАТО."),
                new CalendarOfEvents(DateTime.Parse("2018.05.10"), "1811 - В Англии введены бумажные деньги."),
                new CalendarOfEvents(DateTime.Parse("2018.05.12"), "1954 - Украина стала членом ЮНЕСКО."),
                new CalendarOfEvents(DateTime.Parse("2018.05.14"), "1948 - Провозглашено создание государства Израиль."),
                new CalendarOfEvents(DateTime.Parse("2018.05.17"), "1497 - Васко да Гама достиг берегов Индии."),
                new CalendarOfEvents(DateTime.Parse("2018.05.20"), "1939 - Компания «Пан Американ» начала доставку авиапочты через Атлантический океан."),
                new CalendarOfEvents(DateTime.Parse("2018.05.22"), "1985 - На киноэкраны США вышел голливудский боевик «Рэмбо: Первая кровь»."),
                new CalendarOfEvents(DateTime.Parse("2018.05.25"), "1996 - В Киеве восстановлен памятник княгине Ольге, разрушенный в 1919 году."),
                new CalendarOfEvents(DateTime.Parse("2018.05.27"), "1921 - Афганистан стал независимым государством."),
                new CalendarOfEvents(DateTime.Parse("2018.05.30"), "1992 - В Киеве создана музыкальная группа «Грин Грей»."),
                new CalendarOfEvents(DateTime.Parse("2018.06.01"), "1995 - На экраны США вышел фильм с участием Арнольда Шварценеггера «Вспомнить всё»."),
                new CalendarOfEvents(DateTime.Parse("2018.06.04"), "1942 - Вторая мировая война: Завершение эвакуации британских войск из Дюнкерка."),
                new CalendarOfEvents(DateTime.Parse("2018.06.07"), "1498 - Христофор Колумб отправился в своё третье путешествие."),
                new CalendarOfEvents(DateTime.Parse("2018.06.10"), "1936 - В СССР основана киностудия «Союзмультфильм»."),
                new CalendarOfEvents(DateTime.Parse("2018.06.14"), "1834 - Американец Айзек Фишер из Спрингфилда (штат Вермонт) запатентовал наждачную бумагу."),
                new CalendarOfEvents(DateTime.Parse("2018.06.18"), "1817 - В Лондоне открыт мост Ватерлоо."),
                new CalendarOfEvents(DateTime.Parse("2018.06.24"), "1934 - Столица Украины перенесена из Харькова в Киев."),
                new CalendarOfEvents(DateTime.Parse("2018.06.28"), "1996 - День Конституции Украины."),
                new CalendarOfEvents(DateTime.Parse("2018.07.03"), "1990 - Премьера фильма «Крепкий орешек 2»."),
                new CalendarOfEvents(DateTime.Parse("2018.07.10"), "1983 - День рождения Гаврилюка Максима, создателя данного приложения."),
                new CalendarOfEvents(DateTime.Parse("2018.07.15"), "1982 - В Англии вышел фильм «Pink Floyd»: «The Wall» (Стена)."),
                new CalendarOfEvents(DateTime.Parse("2018.07.21"), "1975 - Завершение эксперимента «Союз-Аполлон»."),
                new CalendarOfEvents(DateTime.Parse("2018.07.26"), "1788 - 11-м штатом США становится штат Нью-Йорк."),
                new CalendarOfEvents(DateTime.Parse("2018.08.02"), "1799 - Найден Розеттский камень."),
                new CalendarOfEvents(DateTime.Parse("2018.08.14"), "1984 - Фирма «IBM» выпустила «PC DOS 3.0»."),
                new CalendarOfEvents(DateTime.Parse("2018.08.24"), "1991 - Украина провозгласила свою независимость."),
                new CalendarOfEvents(DateTime.Parse("2018.08.31"), "1900 - Начались продажи «Кока-Колы» в Британии."),
                new CalendarOfEvents(DateTime.Parse("2018.09.07"), "1926 - Испания покинула Лигу Наций."),
                new CalendarOfEvents(DateTime.Parse("2018.09.14"), "1812 - Войска Наполеона вошли в охваченную пожаром Москву."),
                new CalendarOfEvents(DateTime.Parse("2018.09.21"), "1949 - Мао Цзэдун провозгласил создание Китайской Народной Республики."),
                new CalendarOfEvents(DateTime.Parse("2018.09.27"), "1961 - Сьерра-Леоне стала сотым государством — членом ООН."),
                new CalendarOfEvents(DateTime.Parse("2018.10.02"), "1991 - Компьютерные гиганты «Эппл» и «Ай-Би-Эм» договорились об объединении."),
                new CalendarOfEvents(DateTime.Parse("2018.10.13"), "1900 - Вышла основная книга Зигмунда Фрейда «Толкование сновидений»."),
                new CalendarOfEvents(DateTime.Parse("2018.10.24"), "1986 - В Лондоне открыт крупнейший в мире магазин грампластинок."),
                new CalendarOfEvents(DateTime.Parse("2018.11.05"), "1935 - В Киеве пускается первый троллейбус."),
                new CalendarOfEvents(DateTime.Parse("2018.11.15"), "1971 - Фирма Intel выпускает свой первый микропроцессор — модель 4004."),
                new CalendarOfEvents(DateTime.Parse("2018.11.29"), "1760 - Франция передает Детройт Британии."),
                new CalendarOfEvents(DateTime.Parse("2018.12.08"), "1931 - В США запатентован коаксиальный кабель."),
                new CalendarOfEvents(DateTime.Parse("2018.12.21"), "2012 - Конец «длинного цикла» в календаре майя."),
                new CalendarOfEvents(DateTime.Parse("2018.12.31"), "1990 - Гари Каспаров выигрывает чемпионат мира по шахматам у Анатолия Карпова.")
            };

        #endregion
    }
}