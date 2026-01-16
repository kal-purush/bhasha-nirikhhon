namespace Codebreaker.ViewModels.Models;

public partial class Field(string[] possibleColors) : ObservableObject
{
    [ObservableProperty]
    private string? _color;

    public string[] PossibleColors { get; init; } = possibleColors;
}