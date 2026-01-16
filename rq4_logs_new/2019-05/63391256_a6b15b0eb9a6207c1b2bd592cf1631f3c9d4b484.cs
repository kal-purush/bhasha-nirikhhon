using MoarUtils.Utils;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace MoarUtils.commands.web {
  public class GetPublicIpAddress {
    /// <summary>
    /// account for possbility of ELB sheilding the public ip
    /// </summary>
    /// <returns></returns>
    public static string Execute(
      HttpContext hc = null
    ) {
      var remoteIpAddress = "";
      var xForwardedFor = "";
      var remoteAddr = "";
      var ip = "";
      try {
        //https://stackoverflow.com/questions/38571032/how-to-get-httpcontext-current-in-asp-net-core
        //TODO: get if not passed

        if ((hc == null) || (hc.Request == null)) {
          hc = HttpContext.Current;
        }
        if ((hc == null) || (hc.Request == null)) {
          return null;
        }

        remoteIpAddress = hc.Request.UserHostAddress;
        if (!string.IsNullOrEmpty(hc.Request.Headers["X-Forwarded-For"])) {
          xForwardedFor = hc.Request.Headers["X-Forwarded-For"];
        }
        if (!string.IsNullOrEmpty(hc.Request.Headers["REMOTE_ADDR"])) {
          remoteAddr = hc.Request.Headers["REMOTE_ADDR"];
        }

        var loIp = new List<string> { xForwardedFor, remoteAddr, remoteIpAddress };
        loIp = loIp.Where(x => !string.IsNullOrWhiteSpace(x)).ToList();
        ip = loIp.FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(ip) && ip.Contains(",")) {
          ip = ip.Split(new char[] { ',' }).First();
        }

        return ip;
      } catch (Exception ex) {
        LogIt.E(ex);
      } finally {
        LogIt.D(JsonConvert.SerializeObject(new {
          ip,
          remoteIpAddress,
          xForwardedFor,
          remoteAddr
        }, Formatting.Indented));
      }
      return null;
    }
  }
}