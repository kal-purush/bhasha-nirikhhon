using Microsoft.AspNetCore.Mvc;
using System.Linq;
using System.Collections.Generic;
using entities.apifinport.Entities;
using apicore.Controllers;
using Microsoft.AspNetCore.Cors;
using System.Security.Claims;
using Microsoft.Extensions.Options;
using System;
using Microsoft.AspNetCore.Authorization;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Cryptography.KeyDerivation;
using utils.apifinport;
using entities.apifinport.Models;
using dal.apifinport.Context;
using bll.apifinport;
using Microsoft.AspNetCore.Authentication.JwtBearer;

namespace api.coreapi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class WalletController : BaseController<User>
    {
        private WalletBLL _WalletBLL;
        public WalletController(FinPortContext context) : base(context)
        {
            this._WalletBLL = new WalletBLL(context);
        }

        [Route("getwallet/{userId}"), HttpGet]
        [Authorize(JwtBearerDefaults.AuthenticationScheme)]
        [EnableCors("CorsPolicy")]
        public ActionResult<JResponseEntity<WalletEntity>> GetWallet(int userId)
        {
            // JResponseEntity<WalletDepositsEntity> RObj = new
            return _WalletBLL.GetWalletData(userId);
        }
    }
}