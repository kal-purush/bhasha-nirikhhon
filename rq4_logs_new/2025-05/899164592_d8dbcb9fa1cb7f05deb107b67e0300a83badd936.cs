using Content.Shared.Dataset;
using Content.Shared.FixedPoint;
﻿using Content.Shared.NPC.Prototypes;
using Content.Shared.Random;
using Content.Shared.Roles;
using Robust.Shared.Audio;
using Robust.Shared.Prototypes;
using Robust.Shared.Serialization.TypeSerializers.Implementations.Custom;

namespace Content.Server.GameTicking.Rules.Components;

[RegisterComponent, Access(typeof(TraitorRuleSystem))]
public sealed partial class HoundRuleComponent : Component
{
    public readonly List<EntityUid> HoundMinds = new();

    /// <summary>
    /// Give this traitor a briefing in chat.
    /// </summary>
    [DataField]
    public bool GiveBriefing = true;

    public int TotalHound => HoundMinds.Count;

    public enum SelectionState
    {
        WaitingForSpawn = 0,
        ReadyToStart = 1,
        Started = 2,
    }

    /// <summary>
    /// Current state of the rule
    /// </summary>
    public SelectionState SelectionStatus = SelectionState.WaitingForSpawn;

    /// <summary>
    /// When should traitors be selected and the announcement made
    /// </summary>
    [DataField(customTypeSerializer: typeof(TimeOffsetSerializer)), ViewVariables(VVAccess.ReadWrite)]
    public TimeSpan? AnnounceAt;

    /// <summary>
    ///     Path to antagonist alert sound.
    /// </summary>
    [DataField]
    public SoundSpecifier GreetSoundNotification = new SoundPathSpecifier("/Audio/Ambience/Antag/headrev_start.ogg");

    /// <summary>
    /// The amount of TC traitors start with.
    /// </summary>
    [DataField]
    public FixedPoint2 StartingBalance = 50;
}