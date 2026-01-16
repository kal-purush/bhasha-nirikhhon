using System;
using System.Collections.Generic;
using System.Text;

namespace Store.Services.Middleware
{
public    class AccessToken
    {
        public string TokenString { get; set; }
        //
        // Summary:
        //     This must be overridden to get the time when this Microsoft.IdentityModel.Tokens.SecurityToken
        //     was Valid.
        public DateTime ValidFrom { get; set; }
        //
        // Summary:
        //     This must be overridden to get the time when this Microsoft.IdentityModel.Tokens.SecurityToken
        //     is no longer Valid.
        public  DateTime ValidTo { get; set; }
    }
}