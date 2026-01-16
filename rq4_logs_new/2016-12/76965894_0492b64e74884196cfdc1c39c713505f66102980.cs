// Copyright (c) Arctium Emulation.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Framework.Packets.Json.Forms;

namespace Framework.Packets.Json.Authentication
{
    [DataContract]
    public class LogonData
    {
        public string this[string inputId] => Inputs.SingleOrDefault(i => i.Id == inputId)?.Value;

        [DataMember(Name = "version")]
        public string Version { get; set; }

        [DataMember(Name = "program_id")]
        public string Program { get; set; }

        [DataMember(Name = "platform_id")]
        public string Platform { get; set; }

        [DataMember(Name = "inputs")]
        public List<FormInputValue> Inputs { get; set; }
    }
}