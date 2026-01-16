using System;
using System.Security;
using System.Security.Cryptography.X509Certificates;
using System.Collections.Generic;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using System.Threading.Tasks;
using Microsoft.Identity.Client;

namespace PSMSAL
{
    /// <summary>
    /// <para type="synopsis">Generate token via MSAL library.</para>
    /// <para type="description">PowerShell module to generate authentication tokens in Entra ID.</para>
    /// </summary>

    /// <example>
    /// <code>
    /// <para>Get token by user credential.</para>
    ///
    /// $Credential = Get-Credential
    /// $Token = Get-PSMSALToken -ClientId xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -Credential $Credential -Authority AzureAdMultipleOrgs
    /// </code>
    /// </example>

    /// <example>
    /// <code>
    /// <para>Get token by DeviceCode.</para>
    ///
    /// $Token = Get-PSMSALToken -ClientId xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -DeviceCode
    /// </code>
    /// </example>

    /// <example>
    /// <code>
    /// <para>Get token by interactive logon.</para>
    ///
    /// $Token = Get-PSMSALToken -ClientId xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -Interactive
    /// </code>
    /// </example>

    /// <example>
    /// <code>
    /// <para>Get token by certificate.</para>
    ///
    /// $Certificate = Get-PfxCertificate -FilePath /Users/mgajda/cert.pfx
    /// $Token = Get-PSMSALToken -ClientId xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -Certificate $Certificate
    /// </code>
    /// </example>

    /// <example>
    /// <code>
    /// <para>Get token by secret.</para>
    ///
    /// $Token = Get-PSMSALToken -ClientId xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -Secret xyz
    /// </code>
    /// </example>

    [Cmdlet(VerbsCommon.Get,"PSMSALToken",DefaultParameterSetName="Public")]
    [OutputType(typeof(Microsoft.Identity.Client.AuthenticationResult))]
    public class GetPSMSALTokenCmdletCommand : PSCmdlet
    {
        [Parameter(
            Mandatory = true,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        public string ClientId { get; set; }

        [Parameter(
            ValueFromPipelineByPropertyName = true)]
        public string TenantId { get; set; }

        [Parameter(
            ValueFromPipelineByPropertyName = true)]
        public string RedirectUri { get; set; } = "https://login.microsoftonline.com/common/oauth2/nativeclient";

        [Parameter(
            ValueFromPipelineByPropertyName = true)]
        [ValidateSet("AzureAdMyOrg", "AzureAdMultipleOrgs")]
        public string Authority { get; set; } = "AzureAdMultipleOrgs";

        [Parameter(
            ValueFromPipelineByPropertyName = true)]
        public string[] Scopes { get; set; } = new string[1] {"https://graph.microsoft.com/.default"};

        [Parameter(
            ValueFromPipelineByPropertyName = true,
            ParameterSetName = "Public-AcquireTokenByUsernamePassword")]
        public PSCredential Credential { get; set; }

        [Parameter(
            ValueFromPipelineByPropertyName = true,
            ParameterSetName = "Public-AcquireTokenWithDeviceCode")]
        public SwitchParameter DeviceCode { get; set; }

        [Parameter(
            ValueFromPipelineByPropertyName = true,
            ParameterSetName = "Public-AcquireTokenInteractive")]
        public SwitchParameter Interactive { get; set; }

        [Parameter(
            ValueFromPipelineByPropertyName = true,
            ParameterSetName = "Confidential-WithCertificate")]
        public X509Certificate2 Certificate { get; set; }

        [Parameter(
            ValueFromPipelineByPropertyName = true,
            ParameterSetName = "Confidential-WithSecret")]
        public string Secret { get; set; }

        [Parameter(
            ValueFromPipelineByPropertyName = true)]
        public SwitchParameter AsSecureString { get; set; }

        // This method gets called once for each cmdlet in the pipeline when the pipeline starts executing
        protected override void BeginProcessing()
        {
            WriteVerbose("Begin!");
            WriteVerbose(this.ParameterSetName);
        }

        // This method will be called for each input received from the pipeline to this cmdlet; if no input is received, this method is not called
        protected override void ProcessRecord()
        {
            //Initialize result
            AuthenticationResult Token = null;

            //Public application
            if(this.ParameterSetName.StartsWith("Public-"))
            {
                //Create application builder
                WriteVerbose("Public application buider");
                var ClientApplicationBuilder = PublicClientApplicationBuilder.Create(ClientId);

                //Set Authority
                //if(this.MyInvocation.BoundParameters.ContainsKey("Authority"))
                if(!(String.IsNullOrEmpty(Authority)))
                {
                    WriteVerbose("WithAuthority");
                    switch(Authority)
                    {
                        case "AzureAdMyOrg":
                            ClientApplicationBuilder.WithAuthority(Microsoft.Identity.Client.AadAuthorityAudience.AzureAdMyOrg);
                            break;
                        case "AzureAdMultipleOrgs":
                            ClientApplicationBuilder.WithAuthority(Microsoft.Identity.Client.AadAuthorityAudience.AzureAdMultipleOrgs);
                            break;
                    }
                }

                //Set RedirectUri
                if(this.MyInvocation.BoundParameters.ContainsKey("RedirectUri"))
                {
                    WriteVerbose("WithRedirectUri");
                    ClientApplicationBuilder.WithRedirectUri(RedirectUri);
                }

                //Set TenantId
                if(this.MyInvocation.BoundParameters.ContainsKey("TenantId"))
                {
                    WriteVerbose("WithTenantId");
                    ClientApplicationBuilder.WithTenantId(TenantId);
                }

                //Build application
                IPublicClientApplication ClientApplication = ClientApplicationBuilder.Build();

                //Set Scopes
                IEnumerable<string> ScopeLists = Scopes;

                //AcquireTokenByUsernamePassword
                if(this.ParameterSetName == "Public-AcquireTokenByUsernamePassword")
                {
                    WriteVerbose("AcquireTokenByUsernamePassword");
                    Token = ClientApplication.AcquireTokenByUsernamePassword(ScopeLists,Credential.UserName,Credential.Password).ExecuteAsync().Result;
                }

                //AcquireTokenWithDeviceCode
                if(this.ParameterSetName == "Public-AcquireTokenWithDeviceCode")
                {
                    WriteVerbose("AcquireTokenWithDeviceCode");
                    Token = ClientApplication.AcquireTokenWithDeviceCode(ScopeLists,deviceCodeResult =>
                    {
                        Console.WriteLine(deviceCodeResult.Message);
                        return Task.FromResult(0);
                    }).ExecuteAsync().Result;
                }

                //AcquireTokenInteractive
                if(this.ParameterSetName == "Public-AcquireTokenInteractive")
                {
                    WriteVerbose("AcquireTokenInteractive");
                    Token = ClientApplication.AcquireTokenInteractive(ScopeLists).ExecuteAsync().Result;
                }

            } else {
                WriteVerbose("Confidential application buider");
                var ClientApplicationBuilder = ConfidentialClientApplicationBuilder.Create(ClientId);

                //Set Authority
                //if(this.MyInvocation.BoundParameters.ContainsKey("Authority"))
                if(!(String.IsNullOrEmpty(Authority)))
                {
                    WriteVerbose("WithAuthority");
                    switch(Authority)
                    {
                        case "AzureAdMyOrg":
                            ClientApplicationBuilder.WithAuthority(Microsoft.Identity.Client.AadAuthorityAudience.AzureAdMyOrg);
                            break;
                        case "AzureAdMultipleOrgs":
                            ClientApplicationBuilder.WithAuthority(Microsoft.Identity.Client.AadAuthorityAudience.AzureAdMultipleOrgs);
                            break;
                    }
                }

                //Set RedirectUri
                if(this.MyInvocation.BoundParameters.ContainsKey("RedirectUri"))
                {
                    WriteVerbose("WithRedirectUri");
                    ClientApplicationBuilder.WithRedirectUri(RedirectUri);
                }

                //Set TenantId
                if(this.MyInvocation.BoundParameters.ContainsKey("TenantId"))
                {
                    WriteVerbose("WithTenantId");
                    ClientApplicationBuilder.WithTenantId(TenantId);
                }

                //Set Certificate
                if(this.MyInvocation.BoundParameters.ContainsKey("Certificate"))
                {
                    WriteVerbose("WithCertificate");
                    ClientApplicationBuilder.WithCertificate(Certificate);
                }

                //Set Secret
                if(this.MyInvocation.BoundParameters.ContainsKey("Secret"))
                {
                    WriteVerbose("WithClientSecret");
                    ClientApplicationBuilder.WithClientSecret(Secret);
                }

                //Build application
                IConfidentialClientApplication ClientApplication = ClientApplicationBuilder.Build();

                //Set Scopes
                IEnumerable<string> ScopeLists = Scopes;

                //AcquireTokenForClient - WithCertificate
                if(this.ParameterSetName == "Confidential-WithCertificate")
                {
                    WriteVerbose("AcquireTokenForClient-WithCertificate");
                    Token = ClientApplication.AcquireTokenForClient(ScopeLists).ExecuteAsync().Result;
                }

                //AcquireTokenForClient - WithClientSecret
                if(this.ParameterSetName == "Confidential-WithClientSecret")
                {
                    WriteVerbose("AcquireTokenForClient-WithClientSecret");
                    Token = ClientApplication.AcquireTokenForClient(ScopeLists).ExecuteAsync().Result;
                }
            }

            //Return token
            if(AsSecureString)
            {
                WriteVerbose("AsSecureString");
                SecureString AccessToken = new SecureString();
                Array.ForEach(Token.AccessToken.ToCharArray(), AccessToken.AppendChar);
                AccessToken.MakeReadOnly();

                WriteObject(AccessToken);
            } else {
                WriteObject(Token);
            }

        }

        // This method will be called once at the end of pipeline execution; if no input is received, this method is not called
        protected override void EndProcessing()
        {
            WriteVerbose("End!");
        }
    }
}