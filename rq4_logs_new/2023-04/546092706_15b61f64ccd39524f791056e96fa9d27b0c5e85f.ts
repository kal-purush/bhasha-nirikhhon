// https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method
// generated from terraform resource schema

import { Construct } from 'constructs';
import * as cdktf from 'cdktf';

// Configuration

export interface AclAuthMethodConfig extends cdktf.TerraformMetaArguments {
  /**
  * Defines whether this ACL Auth Method is to be set as default.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#default AclAuthMethod#default}
  */
  readonly default?: boolean | cdktf.IResolvable;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#id AclAuthMethod#id}
  *
  * Please be aware that the id field is automatically added to all resources in Terraform providers using a Terraform provider SDK version below 2.
  * If you experience problems setting this value it might not be settable. Please take a look at the provider documentation to ensure it should be settable.
  */
  readonly id?: string;
  /**
  * Defines the maximum life of a token created by this method.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#max_token_ttl AclAuthMethod#max_token_ttl}
  */
  readonly maxTokenTtl: string;
  /**
  * The identifier of the ACL Auth Method.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#name AclAuthMethod#name}
  */
  readonly name: string;
  /**
  * Defines whether the ACL Auth Method creates a local or global token when performing SSO login. This field must be set to either "local" or "global".
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#token_locality AclAuthMethod#token_locality}
  */
  readonly tokenLocality: string;
  /**
  * ACL Auth Method SSO workflow type. Currently, the only supported type is "OIDC."
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#type AclAuthMethod#type}
  */
  readonly type: string;
  /**
  * config block
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#config AclAuthMethod#config}
  */
  readonly config: AclAuthMethodConfigA;
}
export interface AclAuthMethodConfigA {
  /**
  * A list of allowed values that can be used for the redirect URI.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#allowed_redirect_uris AclAuthMethod#allowed_redirect_uris}
  */
  readonly allowedRedirectUris: string[];
  /**
  * List of auth claims that are valid for login.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#bound_audiences AclAuthMethod#bound_audiences}
  */
  readonly boundAudiences?: string[];
  /**
  * Mappings of claims (key) that will be copied to a metadata field (value).
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#claim_mappings AclAuthMethod#claim_mappings}
  */
  readonly claimMappings?: { [key: string]: string };
  /**
  * PEM encoded CA certs for use by the TLS client used to talk with the OIDC Discovery URL.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#discovery_ca_pem AclAuthMethod#discovery_ca_pem}
  */
  readonly discoveryCaPem?: string[];
  /**
  * Mappings of list claims (key) that will be copied to a metadata field (value).
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#list_claim_mappings AclAuthMethod#list_claim_mappings}
  */
  readonly listClaimMappings?: { [key: string]: string };
  /**
  * The OAuth Client ID configured with the OIDC provider.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#oidc_client_id AclAuthMethod#oidc_client_id}
  */
  readonly oidcClientId: string;
  /**
  * The OAuth Client Secret configured with the OIDC provider.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#oidc_client_secret AclAuthMethod#oidc_client_secret}
  */
  readonly oidcClientSecret: string;
  /**
  * The OIDC Discovery URL, without any .well-known component (base path).
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#oidc_discovery_url AclAuthMethod#oidc_discovery_url}
  */
  readonly oidcDiscoveryUrl: string;
  /**
  * List of OIDC scopes.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#oidc_scopes AclAuthMethod#oidc_scopes}
  */
  readonly oidcScopes?: string[];
  /**
  * A list of supported signing algorithms.
  * 
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method#signing_algs AclAuthMethod#signing_algs}
  */
  readonly signingAlgs?: string[];
}

export function aclAuthMethodConfigAToTerraform(struct?: AclAuthMethodConfigAOutputReference | AclAuthMethodConfigA): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    allowed_redirect_uris: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.allowedRedirectUris),
    bound_audiences: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.boundAudiences),
    claim_mappings: cdktf.hashMapper(cdktf.stringToTerraform)(struct!.claimMappings),
    discovery_ca_pem: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.discoveryCaPem),
    list_claim_mappings: cdktf.hashMapper(cdktf.stringToTerraform)(struct!.listClaimMappings),
    oidc_client_id: cdktf.stringToTerraform(struct!.oidcClientId),
    oidc_client_secret: cdktf.stringToTerraform(struct!.oidcClientSecret),
    oidc_discovery_url: cdktf.stringToTerraform(struct!.oidcDiscoveryUrl),
    oidc_scopes: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.oidcScopes),
    signing_algs: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.signingAlgs),
  }
}

export class AclAuthMethodConfigAOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string) {
    super(terraformResource, terraformAttribute, false, 0);
  }

  public get internalValue(): AclAuthMethodConfigA | undefined {
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._allowedRedirectUris !== undefined) {
      hasAnyValues = true;
      internalValueResult.allowedRedirectUris = this._allowedRedirectUris;
    }
    if (this._boundAudiences !== undefined) {
      hasAnyValues = true;
      internalValueResult.boundAudiences = this._boundAudiences;
    }
    if (this._claimMappings !== undefined) {
      hasAnyValues = true;
      internalValueResult.claimMappings = this._claimMappings;
    }
    if (this._discoveryCaPem !== undefined) {
      hasAnyValues = true;
      internalValueResult.discoveryCaPem = this._discoveryCaPem;
    }
    if (this._listClaimMappings !== undefined) {
      hasAnyValues = true;
      internalValueResult.listClaimMappings = this._listClaimMappings;
    }
    if (this._oidcClientId !== undefined) {
      hasAnyValues = true;
      internalValueResult.oidcClientId = this._oidcClientId;
    }
    if (this._oidcClientSecret !== undefined) {
      hasAnyValues = true;
      internalValueResult.oidcClientSecret = this._oidcClientSecret;
    }
    if (this._oidcDiscoveryUrl !== undefined) {
      hasAnyValues = true;
      internalValueResult.oidcDiscoveryUrl = this._oidcDiscoveryUrl;
    }
    if (this._oidcScopes !== undefined) {
      hasAnyValues = true;
      internalValueResult.oidcScopes = this._oidcScopes;
    }
    if (this._signingAlgs !== undefined) {
      hasAnyValues = true;
      internalValueResult.signingAlgs = this._signingAlgs;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: AclAuthMethodConfigA | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this._allowedRedirectUris = undefined;
      this._boundAudiences = undefined;
      this._claimMappings = undefined;
      this._discoveryCaPem = undefined;
      this._listClaimMappings = undefined;
      this._oidcClientId = undefined;
      this._oidcClientSecret = undefined;
      this._oidcDiscoveryUrl = undefined;
      this._oidcScopes = undefined;
      this._signingAlgs = undefined;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this._allowedRedirectUris = value.allowedRedirectUris;
      this._boundAudiences = value.boundAudiences;
      this._claimMappings = value.claimMappings;
      this._discoveryCaPem = value.discoveryCaPem;
      this._listClaimMappings = value.listClaimMappings;
      this._oidcClientId = value.oidcClientId;
      this._oidcClientSecret = value.oidcClientSecret;
      this._oidcDiscoveryUrl = value.oidcDiscoveryUrl;
      this._oidcScopes = value.oidcScopes;
      this._signingAlgs = value.signingAlgs;
    }
  }

  // allowed_redirect_uris - computed: false, optional: false, required: true
  private _allowedRedirectUris?: string[]; 
  public get allowedRedirectUris() {
    return this.getListAttribute('allowed_redirect_uris');
  }
  public set allowedRedirectUris(value: string[]) {
    this._allowedRedirectUris = value;
  }
  // Temporarily expose input value. Use with caution.
  public get allowedRedirectUrisInput() {
    return this._allowedRedirectUris;
  }

  // bound_audiences - computed: false, optional: true, required: false
  private _boundAudiences?: string[]; 
  public get boundAudiences() {
    return this.getListAttribute('bound_audiences');
  }
  public set boundAudiences(value: string[]) {
    this._boundAudiences = value;
  }
  public resetBoundAudiences() {
    this._boundAudiences = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get boundAudiencesInput() {
    return this._boundAudiences;
  }

  // claim_mappings - computed: false, optional: true, required: false
  private _claimMappings?: { [key: string]: string }; 
  public get claimMappings() {
    return this.getStringMapAttribute('claim_mappings');
  }
  public set claimMappings(value: { [key: string]: string }) {
    this._claimMappings = value;
  }
  public resetClaimMappings() {
    this._claimMappings = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get claimMappingsInput() {
    return this._claimMappings;
  }

  // discovery_ca_pem - computed: false, optional: true, required: false
  private _discoveryCaPem?: string[]; 
  public get discoveryCaPem() {
    return this.getListAttribute('discovery_ca_pem');
  }
  public set discoveryCaPem(value: string[]) {
    this._discoveryCaPem = value;
  }
  public resetDiscoveryCaPem() {
    this._discoveryCaPem = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get discoveryCaPemInput() {
    return this._discoveryCaPem;
  }

  // list_claim_mappings - computed: false, optional: true, required: false
  private _listClaimMappings?: { [key: string]: string }; 
  public get listClaimMappings() {
    return this.getStringMapAttribute('list_claim_mappings');
  }
  public set listClaimMappings(value: { [key: string]: string }) {
    this._listClaimMappings = value;
  }
  public resetListClaimMappings() {
    this._listClaimMappings = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get listClaimMappingsInput() {
    return this._listClaimMappings;
  }

  // oidc_client_id - computed: false, optional: false, required: true
  private _oidcClientId?: string; 
  public get oidcClientId() {
    return this.getStringAttribute('oidc_client_id');
  }
  public set oidcClientId(value: string) {
    this._oidcClientId = value;
  }
  // Temporarily expose input value. Use with caution.
  public get oidcClientIdInput() {
    return this._oidcClientId;
  }

  // oidc_client_secret - computed: false, optional: false, required: true
  private _oidcClientSecret?: string; 
  public get oidcClientSecret() {
    return this.getStringAttribute('oidc_client_secret');
  }
  public set oidcClientSecret(value: string) {
    this._oidcClientSecret = value;
  }
  // Temporarily expose input value. Use with caution.
  public get oidcClientSecretInput() {
    return this._oidcClientSecret;
  }

  // oidc_discovery_url - computed: false, optional: false, required: true
  private _oidcDiscoveryUrl?: string; 
  public get oidcDiscoveryUrl() {
    return this.getStringAttribute('oidc_discovery_url');
  }
  public set oidcDiscoveryUrl(value: string) {
    this._oidcDiscoveryUrl = value;
  }
  // Temporarily expose input value. Use with caution.
  public get oidcDiscoveryUrlInput() {
    return this._oidcDiscoveryUrl;
  }

  // oidc_scopes - computed: false, optional: true, required: false
  private _oidcScopes?: string[]; 
  public get oidcScopes() {
    return this.getListAttribute('oidc_scopes');
  }
  public set oidcScopes(value: string[]) {
    this._oidcScopes = value;
  }
  public resetOidcScopes() {
    this._oidcScopes = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get oidcScopesInput() {
    return this._oidcScopes;
  }

  // signing_algs - computed: false, optional: true, required: false
  private _signingAlgs?: string[]; 
  public get signingAlgs() {
    return this.getListAttribute('signing_algs');
  }
  public set signingAlgs(value: string[]) {
    this._signingAlgs = value;
  }
  public resetSigningAlgs() {
    this._signingAlgs = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get signingAlgsInput() {
    return this._signingAlgs;
  }
}

/**
* Represents a {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method nomad_acl_auth_method}
*/
export class AclAuthMethod extends cdktf.TerraformResource {

  // =================
  // STATIC PROPERTIES
  // =================
  public static readonly tfResourceType = "nomad_acl_auth_method";

  // ===========
  // INITIALIZER
  // ===========

  /**
  * Create a new {@link https://registry.terraform.io/providers/hashicorp/nomad/1.4.20/docs/resources/acl_auth_method nomad_acl_auth_method} Resource
  *
  * @param scope The scope in which to define this construct
  * @param id The scoped construct ID. Must be unique amongst siblings in the same scope
  * @param options AclAuthMethodConfig
  */
  public constructor(scope: Construct, id: string, config: AclAuthMethodConfig) {
    super(scope, id, {
      terraformResourceType: 'nomad_acl_auth_method',
      terraformGeneratorMetadata: {
        providerName: 'nomad',
        providerVersion: '1.4.20',
        providerVersionConstraint: '~> 1.4'
      },
      provider: config.provider,
      dependsOn: config.dependsOn,
      count: config.count,
      lifecycle: config.lifecycle,
      provisioners: config.provisioners,
      connection: config.connection,
      forEach: config.forEach
    });
    this._default = config.default;
    this._id = config.id;
    this._maxTokenTtl = config.maxTokenTtl;
    this._name = config.name;
    this._tokenLocality = config.tokenLocality;
    this._type = config.type;
    this._config.internalValue = config.config;
  }

  // ==========
  // ATTRIBUTES
  // ==========

  // default - computed: false, optional: true, required: false
  private _default?: boolean | cdktf.IResolvable; 
  public get default() {
    return this.getBooleanAttribute('default');
  }
  public set default(value: boolean | cdktf.IResolvable) {
    this._default = value;
  }
  public resetDefault() {
    this._default = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get defaultInput() {
    return this._default;
  }

  // id - computed: true, optional: true, required: false
  private _id?: string; 
  public get id() {
    return this.getStringAttribute('id');
  }
  public set id(value: string) {
    this._id = value;
  }
  public resetId() {
    this._id = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get idInput() {
    return this._id;
  }

  // max_token_ttl - computed: false, optional: false, required: true
  private _maxTokenTtl?: string; 
  public get maxTokenTtl() {
    return this.getStringAttribute('max_token_ttl');
  }
  public set maxTokenTtl(value: string) {
    this._maxTokenTtl = value;
  }
  // Temporarily expose input value. Use with caution.
  public get maxTokenTtlInput() {
    return this._maxTokenTtl;
  }

  // name - computed: false, optional: false, required: true
  private _name?: string; 
  public get name() {
    return this.getStringAttribute('name');
  }
  public set name(value: string) {
    this._name = value;
  }
  // Temporarily expose input value. Use with caution.
  public get nameInput() {
    return this._name;
  }

  // token_locality - computed: false, optional: false, required: true
  private _tokenLocality?: string; 
  public get tokenLocality() {
    return this.getStringAttribute('token_locality');
  }
  public set tokenLocality(value: string) {
    this._tokenLocality = value;
  }
  // Temporarily expose input value. Use with caution.
  public get tokenLocalityInput() {
    return this._tokenLocality;
  }

  // type - computed: false, optional: false, required: true
  private _type?: string; 
  public get type() {
    return this.getStringAttribute('type');
  }
  public set type(value: string) {
    this._type = value;
  }
  // Temporarily expose input value. Use with caution.
  public get typeInput() {
    return this._type;
  }

  // config - computed: false, optional: false, required: true
  private _config = new AclAuthMethodConfigAOutputReference(this, "config");
  public get config() {
    return this._config;
  }
  public putConfig(value: AclAuthMethodConfigA) {
    this._config.internalValue = value;
  }
  // Temporarily expose input value. Use with caution.
  public get configInput() {
    return this._config.internalValue;
  }

  // =========
  // SYNTHESIS
  // =========

  protected synthesizeAttributes(): { [name: string]: any } {
    return {
      default: cdktf.booleanToTerraform(this._default),
      id: cdktf.stringToTerraform(this._id),
      max_token_ttl: cdktf.stringToTerraform(this._maxTokenTtl),
      name: cdktf.stringToTerraform(this._name),
      token_locality: cdktf.stringToTerraform(this._tokenLocality),
      type: cdktf.stringToTerraform(this._type),
      config: aclAuthMethodConfigAToTerraform(this._config.internalValue),
    };
  }
}