using KlockanAPI.Application.DTOs.Meeting;
using KlockanAPI.Application.DTOs.User;
using KlockanAPI.Infrastructure.Repositories.Interfaces;

namespace KlockanAPI.Application.Services;

public class MeetingDetailsService
{
    private readonly IClassroomRepository _classroomRepository;

    public MeetingDetailsService(IClassroomRepository classroomRepository)
    {
        _classroomRepository = classroomRepository;
    }

    public async Task<MeetingDetailsDTO> GetMeetingDetailsAsync(int classroomId)
    {
        var classroom = await _classroomRepository.GetClassroomDetailsAsync(classroomId);
        if (classroom == null)
        {
            throw new KeyNotFoundException($"Classroom with ID {classroomId} not found.");
        }

        var meetingDetails = new MeetingDetailsDTO
        {
            CourseName = classroom.Course.Name,
            ProgramName = classroom.Program.Name,
            Users = new List<UserMeetingDTO>(),
            Sessions = classroom.Course.Sessions ?? 1,
            Duration = classroom.Course.SessionDuration ?? 60,
        };

        foreach (var classroomUser in classroom.ClassroomUsers)
        {
            var userDto = new UserMeetingDTO
            {
                FullName = $"{classroomUser.User.FirstName} {classroomUser.User.LastName}",
                Email = classroomUser.User.Email,
                Role = classroomUser.Role.Name
            };
            meetingDetails.Users.Add(userDto);
        }

        return meetingDetails;
    }
}