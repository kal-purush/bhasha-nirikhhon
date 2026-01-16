#-*- coding: utf8 -*-
from datetime import datetime,date
from stravaweblib  import *
from authorization_data import STRAVA_LOGIN,STRAVA_PASSWORD

import unittest
import warnings

warnings.simplefilter("ignore", ResourceWarning)


class Strava(unittest.TestCase):
    '''
    setUp – подготовка прогона теста; вызывается перед каждым тестом.
    tearDown – вызывается после того, как тест был запущен и результат записан.
               Метод запускается даже в случае исключения (exception) в теле теста.
    setUpClass – метод вызывается перед запуском всех тестов класса.
    tearDownClass – вызывается после прогона всех тестов класса.
    setUpModule – вызывается перед запуском всех классов модуля.
    tearDownModule – вызывается после прогона всех тестов модуля.
    '''
    @classmethod
    def setUpClass(cls):
        pass
    
    def setUp(self):
        self.iam_id = '19098301' # Strava Developer
        self.STRAVA_LOGIN         = STRAVA_LOGIN
        self.STRAVA_PASSWORD      = STRAVA_PASSWORD
        #test athlethes and activity id
        self.test_id = '19600970' # Jane Lane
        self.test_act_id = '947868498' # Jane Lane Maximka
        self.test_act_id_not_kudosed  = '1000770171' # Jane Lane Yash Ride
        pass
        warnings.simplefilter("ignore", ResourceWarning)
        self.strava_client = StravaWebClient()
        self.login = self.strava_client.login(self.STRAVA_LOGIN, self.STRAVA_PASSWORD)

    def tearDown(self):
        pass
    '''
    Суть каждого теста - вызов assertEqual() для проверки ожидаемого результата;
    assertTrue() или assertFalse() для проверки условия;
    assertRaises() для проверки, что метод порождает исключение.
    '''
    def test1_login(self):
        #warnings.simplefilter("ignore", ResourceWarning)
        #strava_client = StravaWebClient()
        signed = self.login #strava_client.login(self.STRAVA_LOGIN, self.STRAVA_PASSWORD)
        self.assertEqual(signed, True)

    def test2_my_name(self):
        strava_client = self.strava_client
        # my name
        name_by_id=strava_client.get_name_athlethe(self.iam_id )
        my_name=strava_client.get_my_name()
        print(my_name+' = Strava Developer', end = " - ")
        self.assertEqual(my_name, 'Strava Developer')
        self.assertEqual(name_by_id, 'Strava Developer')
        
    def test3_name_athlethe(self):
        strava_client = self.strava_client
        name=strava_client.get_name_athlethe(self.test_id )
        print(name+' = Jane Lane', end = " - ")
        self.assertEqual(name, 'Jane Lane')
        
    def test4_activity_data(self):
        strava_client = self.strava_client        
        name_act_by_id=strava_client.get_activity_data(self.test_act_id)
        print(name_act_by_id.get('name')+' = Maximka', end = " - ")
        self.assertEqual(name_act_by_id.get('name'), 'Maximka')
           
    def test5_is_kudosable(self):
        strava_client = self.strava_client
        
        kudosable_alredy_kudosed = strava_client.is_kudosable_activity(self.test_act_id)
        kudosable_not_kudosed = strava_client.is_kudosable_activity(self.test_act_id_not_kudosed)

        print(self.test_act_id+' kudosable = '+str(kudosable_alredy_kudosed), end = " ; ")
        self.assertEqual(kudosable_alredy_kudosed, False)
        print(self.test_act_id_not_kudosed+' kudosable = '+str(kudosable_not_kudosed), end = " - ")
        self.assertEqual(kudosable_not_kudosed, True)
        
    def test6_give_kudo(self):
        strava_client = self.strava_client
        
        kudosable = strava_client.is_kudosable_activity(self.test_act_id)
        print(str(kudosable)+' = kudosable', end = " - ")
        if kudosable :
            strava_client.give_kudo(self.test_act_id)
            print(' Give kudo', end='-')
        self.assertEqual(kudosable, kudosable)
    def test7_get_kudos(self):
        strava_client = self.strava_client
        kudos_list = strava_client.get_kudos(self.test_act_id)
        print(' Get kudos', end='-')
        original_kudos = [{'id': 19098301, 'url': '/athletes/19098301', 'name': 'Strava Developer', 'firstname': 'Strava', 'avatar_url': 'https://dgalywyr863hv.cloudfront.net/pictures/athletes/19098301/5456574/1/medium.jpg', 'member_type': '', 'location': 'Amsterdam, Noord-Holland, Netherlands', 'is_private': False, 'is_following': True}]
        #print(kudos_list)
        #print(original_kudos)
        self.assertEqual(kudos_list, original_kudos)

    def test8_get_activities_by_interval(self):
        strava_client = self.strava_client
        activity_list = strava_client.get_activities_by_interval(self.test_id ,
                                                                 '2016', '03',
                                                                 'month')
        print(' Get activity 2016 03 month', end='-')
        original_activity_list = ['1000795688', '1000770171']
        self.assertEqual(activity_list, original_activity_list)

    def test9_get_last_activities(self):
        strava_client = self.strava_client
        activity_list = strava_client.get_last_activities(self.test_id)
        print(' Get last activities', end='-')
        #self.assertEqual(activity_list, original_activity_list)


if __name__ == '__main__':
    unittest.main(verbosity=2)

   