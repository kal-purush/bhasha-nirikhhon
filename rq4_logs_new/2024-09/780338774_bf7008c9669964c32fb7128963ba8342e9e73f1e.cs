using MediatR;

namespace MB.Application.Features.Moods.Commands.CreateMoodVideo;

public class CreateMoodVideoCommand : IRequest<CreateMoodVideoCommandResponse>
{
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public int Size { get; set; }
    public string Extension { get; set; } = string.Empty;
    public int Height { get; set; }
    public int Width { get; set; }
    public int Duration { get; set; }
    public string SourceFile { get; set; } = string.Empty;
}