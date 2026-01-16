import pytest

from src.class_vacansy import Vacansy


@pytest.fixture
def vacansy():
    vacs_list = Vacansy('Разработчик', 400_000, 'Москва')
    return vacs_list


def test_init(vacansy):
    assert vacansy.name == 'Разработчик'
    assert vacansy.salary == 400000
    assert vacansy.city == 'Москва'
    assert vacansy.vacansy() == [{'id': '94212812', 'premium': False, 'name': 'Ведущий программист 1С (Москва)',
                                  'department': None, 'has_test': False, 'response_letter_required': False,
                                  'area': {'id': '1', 'name': 'Москва', 'url': 'https://api.hh.ru/areas/1'},
                                  'salary': {'from': 460000, 'to': 517000, 'currency': 'RUR', 'gross': False},
                                  'type': {'id': 'open', 'name': 'Открытая'}, 'address': None, 'response_url': None,
                                  'sort_point_distance': None, 'published_at': '2024-03-04T15:02:34+0300',
                                  'created_at': '2024-03-04T15:02:34+0300', 'archived': False,
                                  'apply_alternate_url': 'https://hh.ru/applicant/vacancy_response?vacancyId=94212812',
                                  'show_logo_in_search': None, 'insider_interview': None,
                                  'url': 'https://api.hh.ru/vacancies/94212812?host=hh.ru',
                                  'alternate_url': 'https://hh.ru/vacancy/94212812', 'relations': [],
                                  'employer': {'id': '195848', 'name': 'Трансстроймеханизация',
                                               'url': 'https://api.hh.ru/employers/195848',
                                               'alternate_url': 'https://hh.ru/employer/195848',
                                               'logo_urls':
                                                   {'original': 'https://hhcdn.ru/employer-logo-original/193024.png',
                                                    '240': 'https://hhcdn.ru/employer-logo/1081713.png',
                                                    '90': 'https://hhcdn.ru/employer-logo/1081712.png'},
                                               'vacancies_url': 'https://api.hh.ru/vacancies?employer_id=195848',
                                               'accredited_it_employer': False, 'trusted': True},
                                  'snippet': {'requirement': 'Образование выше среднего. '
                                                             'Наличие сертификатов 1С: Специалист по '
                                                             'платформе – приветствуется. '
                                                             'Знание 1С:ERP (производство и ремонты) - приветствуется. '
                                                             '',
                                              'responsibility': 'Доработка конфигурации под требования организации. '
                                                                'Выяснение причин поведения программы. '
                                                                'Поиск и исправление ошибок. Оптимизация кода/запросов. '
                                                                'Подготовка к обновлению релизов.'},
                                  'contacts': None, 'schedule': {'id': 'fullDay', 'name': 'Полный день'},
                                  'working_days': [], 'working_time_intervals': [], 'working_time_modes': [],
                                  'accept_temporary': False,
                                  'professional_roles': [{'id': '96', 'name': 'Программист, разработчик'}],
                                  'accept_incomplete_resumes': False,
                                  'experience': {'id': 'between1And3', 'name': 'От 1 года до 3 лет'},
                                  'employment': {'id': 'full', 'name': 'Полная занятость'},
                                  'adv_response_url': None, 'is_adv_vacancy': False, 'adv_context': None}]

    assert vacansy.__str__() == ('1.Ведущий программист 1С (Москва), Зарплата от: 460000, Зарплата до: 517000, '
                                 'Город: Москва, Ссылка на вакансию: https://hh.ru/vacancy/94212812')
    assert vacansy.top_vacansy() == 'Найдены вакансии в колличестве 1, с максимальной зарплатой: 460000:'