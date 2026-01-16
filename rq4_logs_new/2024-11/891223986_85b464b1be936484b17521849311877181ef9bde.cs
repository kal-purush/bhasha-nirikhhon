using System;
using SnackHub.ClientService.Domain.ValueObjects;

namespace SnackHub.ClientService.Domain.Entities;

public record ClientModel
{
    private ClientModel()
    {
    }

    public Guid Id { get; init; }
    public string Name { get; init; }
    public string Cpf { get; init; }
    public string Email { get; init; }

    public static ClientModel Create(Guid id, string name, string cpf, string email)
    {
        return new ClientModel()
        {
            Id = id,
            Name = name,
            Cpf = cpf,
            Email = email
        };
    }

    public Client ToDomainModel()
    {
        return new Client(
            Id,
            Name,
            new Cpf(Cpf),
            Email);
    }
}