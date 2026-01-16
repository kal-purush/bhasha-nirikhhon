using Domain.VirtualMachine.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain.VirtualMachine.Models;

public class VirtualMachineReplica
{
    public required string BuildId { get; init; }

    public required string Id { get; init; }

    public required string Rev { get; init; }

    public required VirtualMachineReplicaState State { get; init; }

    public required Dictionary<string, string> Labels { get; init; }
}