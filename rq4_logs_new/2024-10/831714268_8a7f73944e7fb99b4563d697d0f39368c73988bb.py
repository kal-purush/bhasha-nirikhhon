"""Models for Portainer API settings."""

from dataclasses import dataclass, field
from typing import List, Optional

from mashumaro import DataClassDictMixin, field_options


@dataclass
class InternalAuthSettings(DataClassDictMixin):
    """Represents internal authentication settings."""

    required_password_length: int = field(
        default=12, metadata=field_options(alias="RequiredPasswordLength")
    )


@dataclass
class TLSConfig(DataClassDictMixin):
    """TLS configuration settings."""

    tls: bool = field(default=False, metadata=field_options(alias="TLS"))
    tls_skip_verify: bool = field(
        default=False, metadata=field_options(alias="TLSSkipVerify")
    )


@dataclass
class SearchSetting(DataClassDictMixin):
    """Settings for searching within a specific LDAP base DN."""

    base_dn: str = field(default="", metadata=field_options(alias="BaseDN"))
    filter: str = field(default="", metadata=field_options(alias="Filter"))
    username_attribute: str = field(
        default="", metadata=field_options(alias="UserNameAttribute")
    )


@dataclass
class GroupSearchSetting(DataClassDictMixin):
    """Settings for LDAP group search configurations."""

    group_base_dn: str = field(default="", metadata=field_options(alias="GroupBaseDN"))
    group_filter: str = field(default="", metadata=field_options(alias="GroupFilter"))
    group_attribute: str = field(
        default="", metadata=field_options(alias="GroupAttribute")
    )


@dataclass
class LDAPSettings(DataClassDictMixin):
    """Settings for LDAP-based authentication."""

    anonymous_mode: bool = field(
        default=True, metadata=field_options(alias="AnonymousMode")
    )
    reader_dn: str = field(default="", metadata=field_options(alias="ReaderDN"))
    url: str = field(default="", metadata=field_options(alias="URL"))
    tls_config: TLSConfig = field(
        default_factory=TLSConfig, metadata=field_options(alias="TLSConfig")
    )
    start_tls: bool = field(default=False, metadata=field_options(alias="StartTLS"))
    search_settings: List[SearchSetting] = field(
        default_factory=list, metadata=field_options(alias="SearchSettings")
    )
    group_search_settings: List[GroupSearchSetting] = field(
        default_factory=list, metadata=field_options(alias="GroupSearchSettings")
    )
    auto_create_users: bool = field(
        default=True, metadata=field_options(alias="AutoCreateUsers")
    )


@dataclass
class OAuthSettings(DataClassDictMixin):
    """Settings for OAuth-based authentication."""

    client_id: str = field(default="", metadata=field_options(alias="ClientID"))
    access_token_uri: str = field(
        default="", metadata=field_options(alias="AccessTokenURI")
    )
    authorization_uri: str = field(
        default="", metadata=field_options(alias="AuthorizationURI")
    )
    resource_uri: str = field(default="", metadata=field_options(alias="ResourceURI"))
    redirect_uri: str = field(default="", metadata=field_options(alias="RedirectURI"))
    user_identifier: str = field(
        default="", metadata=field_options(alias="UserIdentifier")
    )
    scopes: str = field(default="", metadata=field_options(alias="Scopes"))
    oauth_auto_create_users: bool = field(
        default=False, metadata=field_options(alias="OAuthAutoCreateUsers")
    )
    default_team_id: int = field(
        default=0, metadata=field_options(alias="DefaultTeamID")
    )
    sso: bool = field(default=True, metadata=field_options(alias="SSO"))
    logout_uri: str = field(default="", metadata=field_options(alias="LogoutURI"))
    kube_secret_key: Optional[str] = field(
        default=None, metadata=field_options(alias="KubeSecretKey")
    )


@dataclass
class OpenAMTConfiguration(DataClassDictMixin):
    """Settings for OpenAMT configuration."""

    enabled: bool = field(default=False, metadata=field_options(alias="enabled"))
    mps_server: str = field(default="", metadata=field_options(alias="mpsServer"))
    mps_user: str = field(default="", metadata=field_options(alias="mpsUser"))
    mps_password: str = field(default="", metadata=field_options(alias="mpsPassword"))
    mps_token: str = field(default="", metadata=field_options(alias="mpsToken"))
    cert_file_name: str = field(
        default="", metadata=field_options(alias="certFileName")
    )
    cert_file_content: str = field(
        default="", metadata=field_options(alias="certFileContent")
    )
    cert_file_password: str = field(
        default="", metadata=field_options(alias="certFilePassword")
    )
    domain_name: str = field(default="", metadata=field_options(alias="domainName"))


@dataclass
class FDOConfiguration(DataClassDictMixin):
    """Settings for FDO (Factory Device Onboarding) configuration."""

    enabled: bool = field(default=False, metadata=field_options(alias="enabled"))
    owner_url: str = field(default="", metadata=field_options(alias="ownerURL"))
    owner_username: str = field(
        default="", metadata=field_options(alias="ownerUsername")
    )
    owner_password: str = field(
        default="", metadata=field_options(alias="ownerPassword")
    )


@dataclass
class EdgeConfiguration(DataClassDictMixin):
    """Settings for Edge computing configurations."""

    command_interval: int = field(
        default=0, metadata=field_options(alias="CommandInterval")
    )
    ping_interval: int = field(default=0, metadata=field_options(alias="PingInterval"))
    snapshot_interval: int = field(
        default=0, metadata=field_options(alias="SnapshotInterval")
    )
    async_mode: bool = field(default=False, metadata=field_options(alias="AsyncMode"))


@dataclass
class PortainerSettings(DataClassDictMixin):
    """Main settings configuration for Portainer."""

    logo_url: str = field(default="", metadata=field_options(alias="LogoURL"))
    blacklisted_labels: List[str] = field(
        default_factory=list, metadata=field_options(alias="BlackListedLabels")
    )
    authentication_method: int = field(
        default=1, metadata=field_options(alias="AuthenticationMethod")
    )
    internal_auth_settings: InternalAuthSettings = field(
        default_factory=InternalAuthSettings,
        metadata=field_options(alias="InternalAuthSettings"),
    )
    ldap_settings: LDAPSettings = field(
        default_factory=LDAPSettings, metadata=field_options(alias="LDAPSettings")
    )
    oauth_settings: OAuthSettings = field(
        default_factory=OAuthSettings, metadata=field_options(alias="OAuthSettings")
    )
    open_amt_configuration: OpenAMTConfiguration = field(
        default_factory=OpenAMTConfiguration,
        metadata=field_options(alias="openAMTConfiguration"),
    )
    fdo_configuration: FDOConfiguration = field(
        default_factory=FDOConfiguration,
        metadata=field_options(alias="fdoConfiguration"),
    )
    feature_flag_settings: Optional[str] = field(
        default=None, metadata=field_options(alias="FeatureFlagSettings")
    )
    snapshot_interval: str = field(
        default="1m", metadata=field_options(alias="SnapshotInterval")
    )
    templates_url: str = field(
        default="https://raw.githubusercontent.com/portainer/templates/master/templates-2.0.json",  # pylint: disable=line-too-long
        metadata=field_options(alias="TemplatesURL"),
    )
    edge_agent_checkin_interval: int = field(
        default=5, metadata=field_options(alias="EdgeAgentCheckinInterval")
    )
    show_kompose_build_option: bool = field(
        default=False, metadata=field_options(alias="ShowKomposeBuildOption")
    )
    enable_edge_compute_features: bool = field(
        default=False, metadata=field_options(alias="EnableEdgeComputeFeatures")
    )
    user_session_timeout: str = field(
        default="8h", metadata=field_options(alias="UserSessionTimeout")
    )
    kubeconfig_expiry: str = field(
        default="0", metadata=field_options(alias="KubeconfigExpiry")
    )
    enable_telemetry: bool = field(
        default=True, metadata=field_options(alias="EnableTelemetry")
    )
    helm_repository_url: str = field(
        default="https://charts.bitnami.com/bitnami",
        metadata=field_options(alias="HelmRepositoryURL"),
    )
    kubectl_shell_image: str = field(
        default="portainer/kubectl-shell",
        metadata=field_options(alias="KubectlShellImage"),
    )
    trust_on_first_connect: bool = field(
        default=False, metadata=field_options(alias="TrustOnFirstConnect")
    )
    enforce_edge_id: bool = field(
        default=False, metadata=field_options(alias="EnforceEdgeID")
    )
    agent_secret: str = field(default="", metadata=field_options(alias="AgentSecret"))
    edge_portainer_url: str = field(
        default="", metadata=field_options(alias="EdgePortainerUrl")
    )
    edge: EdgeConfiguration = field(
        default_factory=EdgeConfiguration, metadata=field_options(alias="Edge")
    )
    display_donation_header: bool = field(
        default=False, metadata=field_options(alias="DisplayDonationHeader")
    )
    display_external_contributors: bool = field(
        default=False, metadata=field_options(alias="DisplayExternalContributors")
    )
    enable_host_management_features: bool = field(
        default=False, metadata=field_options(alias="EnableHostManagementFeatures")
    )
    allow_volume_browser_for_regular_users: bool = field(
        default=False, metadata=field_options(alias="AllowVolumeBrowserForRegularUsers")
    )
    allow_bind_mounts_for_regular_users: bool = field(
        default=False, metadata=field_options(alias="AllowBindMountsForRegularUsers")
    )
    allow_privileged_mode_for_regular_users: bool = field(
        default=False,
        metadata=field_options(alias="AllowPrivilegedModeForRegularUsers"),
    )
    allow_host_namespace_for_regular_users: bool = field(
        default=False, metadata=field_options(alias="AllowHostNamespaceForRegularUsers")
    )
    allow_stack_management_for_regular_users: bool = field(
        default=False,
        metadata=field_options(alias="AllowStackManagementForRegularUsers"),
    )
    allow_device_mapping_for_regular_users: bool = field(
        default=False, metadata=field_options(alias="AllowDeviceMappingForRegularUsers")
    )
    allow_container_capabilities_for_regular_users: bool = field(
        default=False,
        metadata=field_options(alias="AllowContainerCapabilitiesForRegularUsers"),
    )
    is_docker_desktop_extension: bool = field(
        default=False, metadata=field_options(alias="IsDockerDesktopExtension")
    )