/*
 * Copyright (c) 2012, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.carepay.jdbc.mysql;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

import com.carepay.aws.auth.CredentialsProvider;
import com.carepay.aws.auth.DefaultCredentialsProviderChain;
import com.carepay.aws.auth.RegionProvider;
import com.carepay.aws.region.DefaultRegionProviderChain;
import com.carepay.jdbc.RdsAWS4Signer;
import com.carepay.jdbc.pem.PemKeyStoreProvider;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.conf.EnumProperty;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

import static java.time.ZoneOffset.UTC;

/**
 * MySQL Clear Password Authentication Plugin
 */
public class RdsIamPasswordPlugin implements AuthenticationPlugin<NativePacketPayload> {
    /** replaces the built-in clear-text plugin */
    public static String PLUGIN_NAME = "mysql_clear_password";

    private Protocol<NativePacketPayload> protocol;

    private final Clock clock;

    private final RdsAWS4Signer signer;
    private LocalDateTime expiryDate;
    protected String username;
    private String password;

    /**
     * Default constructor, uses AWS provider-chain by default
     */
    public RdsIamPasswordPlugin() {
        this(new DefaultCredentialsProviderChain(), new DefaultRegionProviderChain(), Clock.systemUTC());
    }

    /**
     * Custom constructor, used for tests.
     *
     * @param credentialsProvider the AWS credentials-provider to use
     * @param regionProvider the AWS region-provider to use
     * @param clock the system-clock used to check if token is expired
     */
    public RdsIamPasswordPlugin(final CredentialsProvider credentialsProvider, final RegionProvider regionProvider, final Clock clock) {
        this.clock = clock;
        this.signer = new RdsAWS4Signer(credentialsProvider, regionProvider, this.clock);
        PemKeyStoreProvider.register();
    }

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        this.protocol = prot;
        final PropertySet propertySet = this.protocol.getPropertySet();
        propertySet.getBooleanProperty(PropertyKey.useSSL).setValue(true);
        propertySet.getBooleanProperty(PropertyKey.verifyServerCertificate).setValue(true);
        ((EnumProperty<PropertyDefinitions.SslMode>) propertySet.getEnumProperty(PropertyKey.sslMode)).setValue(PropertyDefinitions.SslMode.VERIFY_CA);
        propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreUrl).setValue("classpath:/rds-combined-ca-bundle.pem");
        propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreType).setValue("PEM");
        propertySet.getStringProperty(PropertyKey.enabledTLSProtocols).setValue("TLSv1.2");
    }

    public void destroy() {
    }

    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    public boolean requiresConfidentiality() {
        return true;
    }

    public boolean isReusable() {
        return true;
    }

    public void setAuthenticationParameters(String user, String password) {
        this.username = user;
    }

    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();
        final String token = getPassword();
        final String encoding = this.protocol.versionMeetsMinimum(5, 7, 6) ? this.protocol.getPasswordCharacterEncoding() : "UTF-8";
        final NativePacketPayload payload = new NativePacketPayload(StringUtils.getBytes(token, encoding));
        payload.setPosition(payload.getPayloadLength());
        payload.writeInteger(IntegerDataType.INT1, 0);
        payload.setPosition(0);
        toServer.add(payload);
        return true;
    }

    /**
     * Gets the IAM token. Creates a new token when the token is expired.
     *
     * @return the IAM RDS token.
     */
    public String getPassword() {
        refreshToken();
        return this.password;
    }

    protected void refreshToken() {
        final LocalDateTime now = LocalDateTime.ofInstant(clock.instant(), UTC);
        if (this.expiryDate == null || expiryDate.isBefore(now)) {
            final String host = this.protocol.getSocketConnection().getHost();
            final int port = this.protocol.getSocketConnection().getPort();
            this.password = signer.generateToken(host, port, username);
            this.expiryDate = now.plusMinutes(10L); // Token expires after 15 min, so refresh after 10 min
        }
    }
}