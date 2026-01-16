using Microsoft.AspNetCore.Mvc;
using MRF.Models.DTO;
using System;
using System.Collections.Generic;
using System.DirectoryServices.Protocols;

namespace MRF.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GetLDAPEmployeeController : ControllerBase
    {
        private readonly ILogger<GetLDAPEmployeeController> _logger; // Change ILoggerService to ILogger
        private readonly IConfiguration _configuration;

        public GetLDAPEmployeeController(ILogger<GetLDAPEmployeeController> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        [HttpGet]
        public ActionResult<IEnumerable<LdapEmployeeDTO>> Get()
        {
            List<LdapEmployeeDTO> ldapEmployees = new List<LdapEmployeeDTO>();

            try
            {
                string ldapServer = _configuration["LDAP:Server"];
                int ldapPort = int.Parse(_configuration["LDAP:Port"]);
                string baseDN = _configuration["LDAP:baseDN"];
                string username = _configuration["LDAP:UserName"];
                string password = _configuration["LDAP:Password"];

                int pageSize = 6000; // Specify the page size
                byte[] cookie = null;
                SearchResultEntryCollection results = null;

                using (LdapConnection ldapConnection = new LdapConnection(ldapServer + ":" + ldapPort))
                {
                    ldapConnection.AuthType = AuthType.Basic;
                    ldapConnection.SessionOptions.ProtocolVersion = 3;
                    ldapConnection.Credential = new System.Net.NetworkCredential(username, password);
                    ldapConnection.Bind();

                    do
                    {
                        SearchRequest searchRequest = new SearchRequest(
                            baseDN,
                            "(objectClass=user)",
                            SearchScope.Subtree,
                           // "sAMAccountName", "mail","description" // Add attributes to fetch here
                           null
                        );

                        PageResultRequestControl pageRequestControl = new PageResultRequestControl(pageSize);
                        if (cookie != null)
                        {
                            pageRequestControl.Cookie = cookie;
                        }
                        searchRequest.Controls.Add(pageRequestControl);

                        SearchResponse searchResponse = (SearchResponse)ldapConnection.SendRequest(searchRequest);

                        results = searchResponse.Entries;

                        foreach (SearchResultEntry entry in results)
                        {
                            var sAMAccountNameAttr = entry.Attributes["sAMAccountName"];
                            var mailAttr = entry.Attributes["mail"];
                            var description = entry.Attributes["description"];
                            if (sAMAccountNameAttr != null && sAMAccountNameAttr.Count > 0
                                && mailAttr != null && mailAttr.Count > 0)
                            {
                                ldapEmployees.Add(new LdapEmployeeDTO
                                {
                                    UserName = sAMAccountNameAttr[0].ToString(),
                                    Email = mailAttr[0].ToString(),
                                    EmployeeId = description[0].ToString()
                                });
                            }
                        }

                        foreach (DirectoryControl control in searchResponse.Controls)
                        {
                            if (control is PageResultResponseControl)
                            {
                                cookie = ((PageResultResponseControl)control).Cookie;
                                break;
                            }
                        }
                    } while (cookie != null && cookie.Length > 0 && results != null && results.Count > 0);
                }

                return Ok(ldapEmployees); // Return the fetched LDAP employees as JSON
            }
            catch (Exception e)
            {
                _logger.LogError($"Error fetching LDAP employees: {e.Message}");
                return StatusCode(500, "Error fetching LDAP employees");
            }
        }
    }
}