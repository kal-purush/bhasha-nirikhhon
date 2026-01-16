// <copyright file="GearboxPacket.cs" company="PlaceholderCompany">
// Copyright (c) PlaceholderCompany. All rights reserved.
// </copyright>

namespace AutomatedCar.SystemComponents.Packets
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using AutomatedCar.SystemComponents.Behaviour;

    /// <summary>
    /// The packed used by the gearbox.
    /// </summary>
    public class GearboxPacket : IGearboxPacket
    {
        /// <inheritdoc/>
        public double Torque { get; set; }

        /// <inheritdoc/>
        public int DriveSubgear { get; set; }

        /// <inheritdoc/>
        public Gear Gear { get; set; }
    }
}