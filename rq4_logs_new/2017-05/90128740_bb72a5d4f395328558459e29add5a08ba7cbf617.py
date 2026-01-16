from ptest.decorator import TestClass, Test, BeforeMethod
from services.booking_service.load_student_booking_info import *


@TestClass(run_mode="singleline")
class VerifyLoadStudentBookingInfo:
    @BeforeMethod
    def before(self):
        self.student_id = 11281836

    @Test
    def test_load_student_booking_info_failed(self):
        begin_date = get_further_date(10)
        end_date = get_further_date(20)
        load_response = load_student_booking_info(self.student_id, begin_date, end_date)
        print(type(load_response))