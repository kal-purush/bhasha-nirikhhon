using MapsterMapper;
using KlockanAPI.Application.Services.Interfaces;
using KlockanAPI.Infrastructure.Repositories.Interfaces;
using KlockanAPI.Application.CrossCutting;
using KlockanAPI.Application.DTOs.ClassroomUser;

namespace KlockanAPI.Application.Services;

public class ClassroomUserService : IClassroomUserService
{
    private readonly IClassroomUserRepository _classroomUserRepository;
    private readonly IClassroomRepository _classroomRepository;
    private readonly IMapper _mapper;

    public ClassroomUserService(IClassroomUserRepository classroomUserRepository, IClassroomRepository classroomRepository, IMapper mapper)
    {
        _classroomUserRepository = classroomUserRepository;
        _classroomRepository = classroomRepository;
        _mapper = mapper;
    }

    public async Task<List<ClassroomUserDTO>> GetUsersByClassroomIdAsync(int classroomId)
    {
        var classroom = await _classroomRepository.GetClassroomByIdAsync(classroomId);
        NotFoundException.ThrowIfNull(classroom, $"Classroom with id {classroomId} not found");

        var schedules = await _classroomUserRepository.GetUsersByClassroomId(classroomId);
        var schedulesDTOs = _mapper.Map<List<ClassroomUserDTO>>(schedules);

        return schedulesDTOs;
    }
}