// Copyright (c) Arctium Emulation.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using LappaORM;

namespace Framework.Database.Bnet
{
    public class Account : Entity
    {
        [AutoIncrement]
        public uint Id                 { get; set; }
        public string Email            { get; set; }
        public string PasswordVerifier { get; set; }
        public string Salt             { get; set; }
        public byte Region             { get; set; }
        public string Locale           { get; set; }
        public string IPv4             { get; set; }
        public string IPv6             { get; set; }
        public uint LoginFailures      { get; set; }

        public virtual List<GameAccount> GameAccounts { get; set; }
    }
}