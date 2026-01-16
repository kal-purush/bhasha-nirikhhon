from ptest.decorator import TestClass, Test, BeforeMethod
from services.booking_service.load_class_booking_detail_info import *


@TestClass(run_mode="singleline")
class LoadClassBookingDetailInfo:
    @BeforeMethod()
    def setup_date(self):
        self.student_id = 11282834

    @Test(tags="Smoke")
    def load_class_booking_detail_info_success(self):
        load_class_booking_detail_info(self.student_id, 3031304)

    @Test(tags="Smoke")
    def load_f2f_level_not_matched_detail_info(self):
        message = load_class_booking_detail_info(self.student_id, 3031333)

        assert_that(message[0]).is_equal_to("NonVisibleScheduledClass")
        assert_that(message[1]).is_equal_to("Current scheduled class is not visible now")

    @Test(tags="Smoke")
    def load_ccb_center_class_booking_detail_info(self):
        load_class_booking_detail_info(self.student_id, 3031328)

    @Test(tags="Smoke")
    def load_other_center_class_booking_detail_info(self):
        message = load_class_booking_detail_info(self.student_id, 3031334)

        assert_that(message[0]).is_equal_to("NonVisibleScheduledClass")
        assert_that(message[1]).is_equal_to("Current scheduled class is not visible now")

    @Test(tags="Smoke")
    def load_invalid_student_id_class_booking_detail_info(self):
        message = load_class_booking_detail_info(-2394, 3031334)

        assert_that(message[0]).is_equal_to("InvalidParam")
        assert_that(message[1]).is_equal_to("InvalidParam")

    @Test(tags="Smoke")
    def load_invalid_scheduled_id_class_booking_detail_info(self):
        message = load_class_booking_detail_info(self.student_id, -3031334)

        assert_that(message[0]).is_equal_to("InvalidParam")
        assert_that(message[1]).is_equal_to("InvalidParam")