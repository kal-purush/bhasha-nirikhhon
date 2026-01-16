using Domain;
using Repository;

namespace UnitTests;

public class DirtyRepositoryTests
{
    private readonly ServiceContext _context;
    private readonly IAppointmentRepository _appoinmentRepository;
    private readonly IDoctorRepository _doctorRepository;
    private readonly IScheduleRepository _scheduleRepository;
    private readonly IUserRepository _userRepository;

    public DirtyRepositoryTests()
    {
        _context = new ServiceContext();
        _appoinmentRepository = new AppointmentRepositoryImpl(_context);
        _doctorRepository = new DoctorRepositoryImpl(_context);
        _scheduleRepository = new ScheduleRepositoryImpl(_context);
        _userRepository = new UserRepositoryImpl(_context);
    }

    [Fact]
    public void AddUser_ShouldOk()
    {
        //Arrange
        string phone = "222-444-e7";
        string name = "Bob Bob Bob";
        string login = "Bob3";
        string password = "strongBob12345";
        var form = new UserForm(phone, name, login, password);
        
        //Act
        var result = _userRepository.CreateUser(form);
        
        //Assert
        Assert.True(result is not null);
        Assert.Equal(form.PhoneNumber, result.PhoneNumber);
        Assert.Equal(form.FullName, result.FullName);
        Assert.Equal(form.Login, result.Login);
        Assert.Equal(form.Password, result.Password);
    }

    [Fact]
    public void AddAppointment_ShouldOk()
    {
        //Arrange
        DateTime start = new DateTime(2020, 10, 1, 10, 10, 0);
        DateTime end = new DateTime(2020, 10, 1, 10, 30, 43);
        int patientID = 14;
        int doctorID = 88880;
        string specialization = "football ball";
        var form = new AppointmentForm(start, end, patientID, doctorID, specialization);
        
        //Act
        var result = _appoinmentRepository.CreateAppointment(form);
        
        //Assert
        Assert.True(result is not null);
        Assert.Equal(form.Start, result.Start);
        Assert.Equal(form.End, result.End);
        Assert.Equal(form.PatientID, result.PatientID);
        Assert.Equal(form.DoctorID, result.DoctorID);
    }

    [Fact]
    public void AddDoctor_ShouldOk()
    {
        //Arrange
        string name = "BOOOOOB";
        string specialization = "OOO";
        var form = new DoctorForm(name, specialization);
        
        //Act
        var result = _doctorRepository.CreateDoctor(form);
        
        //Assert
        Assert.True(result is not null);
        Assert.Equal(form.FullName, result.FullName);
        Assert.Equal(form.Specialization, result.Specialization);
    }

    [Fact]
    public void AddSchedule_ShouldOk()
    {
        //Arrange
        int doctorID = 123;
        DateOnly date = new DateOnly(2022, 2, 22);
        TimeOnly start = new TimeOnly(0, 0, 0);
        TimeOnly end = new TimeOnly(23, 59, 01);
        var form = new ScheduleForm(doctorID, date, start, end);
        
        //Act
        var result = _scheduleRepository.AddSchedule(form);
        
        //Assert
        Assert.True(result is not null);
        Assert.Equal(form.DoctorID, result.DoctorID);
        Assert.Equal(form.Date, result.Date);
        Assert.Equal(form.DayStart, result.DayStart);
        Assert.Equal(form.DayEnd, result.DayEnd);
    }

    [Fact]
    public void AddAndChangeSchedule_ShouldOk()
    {
        //Arrange
        int doctorID = 123;
        DateOnly actualDate = new DateOnly(2022, 2, 22);
        DateOnly recentDate = new DateOnly(450, 1, 1);
        TimeOnly start = new TimeOnly(0, 0, 0);
        TimeOnly end = new TimeOnly(23, 59, 01);
        var actual = new ScheduleForm(doctorID, actualDate, start, end);
        var recent = new ScheduleForm(doctorID, recentDate, start, end);
        
        //Act
        _scheduleRepository.AddSchedule(actual);
        var result = _scheduleRepository.ChangeSchedule(actual, recent);
        
        //Assert
        Assert.True(result is not null);
        Assert.Equal(recent.DoctorID, result.DoctorID);
        Assert.Equal(recent.Date, result.Date);
        Assert.Equal(recent.DayStart, result.DayStart);
        Assert.Equal(recent.DayEnd, result.DayEnd);
    }
}