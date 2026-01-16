import unittest
from .modules.logic_layer_new import LessonMngr
from .modules.create_schema import Lesson
from .conf.setting import engine

class TestLesson(unittest.TestCase):
    def test_add(self):
        user_response = {'Lesson': {'name': 'testday3'}, 'Course': {'name': 'python'}}
        lesson = LessonMngr(user_response)
        lesson_test = lesson.add()
        column_values = user_response['Lesson']
        
        lesson_answer = lesson.session.query(Lesson).filter_by(**column_values).one_or_none()
        self.assertEqual(lesson_test, lesson_answer)

if __name__ == '__main__':
    unittest.main()