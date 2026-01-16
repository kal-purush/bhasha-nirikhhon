package com.mocktest.mockito;

import com.google.gson.JsonSyntaxException;
import com.mocktest.TeacherConst;
import com.mocktest.TeacherOfficeHoursController;
import com.mocktest.TeacherOfficeHoursModel;
import com.mocktest.TeacherOfficeHoursService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TestTeacherOffice {
    @Mock
    private TeacherOfficeHoursService service;

    private TeacherOfficeHoursController controller;

    @Before
    public void setUp() {
        controller = new TeacherOfficeHoursController(service);
    }

    @Test
    public void testGetTeacherOfficeHoursInfo() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_1);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        TeacherOfficeHoursModel teacher = controller.getTeacherOfficeHoursInfo("John Doe");
        assertNotNull(teacher);
        assertEquals(teacher.name, "John Doe");
        assertEquals(teacher.room, 1);
        assertEquals(teacher.officeHour, "10:00 AM - 11:00 AM");
    }

    @Test
    public void testGetTeacherOfficeHoursInfoWhenTheresMoreThanOne() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_1, TeacherConst.TEACHER_2);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        TeacherOfficeHoursModel teacher = controller.getTeacherOfficeHoursInfo("John Doe");
        assertNotNull(teacher);
        assertEquals(teacher.name, "John Doe");
        assertEquals(teacher.room, 1);
        assertEquals(teacher.officeHour, "10:00 AM - 11:00 AM");
    }

    @Test
    public void testGetWrongTeacherOfficeHoursInfo() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_3, TeacherConst.TEACHER_4);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        TeacherOfficeHoursModel teacher = controller.getTeacherOfficeHoursInfo("John Doe");
        assertNull(teacher);
    }

    @Test(expected = JsonSyntaxException.class)
    public void testFetchDataOfSingleJSONObject() {
        Mockito.when(service.fetchData()).thenReturn(TeacherConst.TEACHER_1);
        controller.getTeacherOfficeHoursInfo("John Doe");
    }

    @Test
    public void testIsTeacherExists() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_5, TeacherConst.TEACHER_6);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        boolean isTeacherExists = controller.isTeacherExists("Diana Evans");
        assertTrue(isTeacherExists);
    }

    @Test
    public void testIsTeacherDontExists() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_5, TeacherConst.TEACHER_6);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        boolean isTeacherExists = controller.isTeacherExists("Bob Brown");
        assertFalse(isTeacherExists);
    }

    @Test
    public void testIsTeacherAvailable() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_6, TeacherConst.TEACHER_7);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        boolean isTeacherAvailable = controller.isTeacherAvailable("Evan Foster", "10:00 AM - 11:00 AM");
        assertTrue(isTeacherAvailable);
    }

    @Test
    public void testIsTeacherNotAvailable() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_7, TeacherConst.TEACHER_8);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        boolean isTeacherAvailable = controller.isTeacherAvailable("Evan Foster", "11:00 AM - 12:00 AM");
        assertFalse(isTeacherAvailable);
    }

    @Test
    public void testIsTeacherAvailableDontExists() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_9, TeacherConst.TEACHER_10);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        boolean isTeacherAvailable = controller.isTeacherAvailable("Evan Foster", "11:00 AM - 12:00 AM");
        assertFalse(isTeacherAvailable);
    }

    @Test
    public void testGetTeacherRoomNumber() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_9, TeacherConst.TEACHER_10);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        int roomNumber = controller.getTeacherRoomNumber("Hannah White");
        assertEquals(roomNumber, 10);
    }

    @Test
    public void testGetWrongTeacherRoomNumber() {
        String teachers = TeacherConst.concatIntoJsonArray(TeacherConst.TEACHER_9, TeacherConst.TEACHER_10);
        Mockito.when(service.fetchData()).thenReturn(teachers);

        int roomNumber = controller.getTeacherRoomNumber("John Doe");
        assertEquals(roomNumber, 0);
    }
}