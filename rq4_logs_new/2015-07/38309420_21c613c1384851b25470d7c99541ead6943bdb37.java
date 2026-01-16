/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.sms.esendex.ecm;

import org.everit.osgi.ecm.annotation.Activate;
import org.everit.osgi.ecm.annotation.Component;
import org.everit.osgi.ecm.annotation.ConfigurationPolicy;
import org.everit.osgi.ecm.annotation.Service;
import org.everit.osgi.ecm.annotation.attribute.StringAttribute;
import org.everit.osgi.ecm.extender.ECMExtenderConstants;
import org.everit.sms.api.SMSSender;
import org.everit.sms.esendex.EsendexSMSSender;

import aQute.bnd.annotation.headers.ProvideCapability;

/**
 * Esendex SMS sender component.
 */
@Component(label = "Esendex SMS Sender Component",
    configurationPolicy = ConfigurationPolicy.REQUIRE)
@ProvideCapability(ns = ECMExtenderConstants.CAPABILITY_NS_COMPONENT,
    value = ECMExtenderConstants.CAPABILITY_ATTR_CLASS + "=${@class}")
@Service(value = { SMSSender.class, EsendexSMSSenderComponent.class })
public class EsendexSMSSenderComponent implements SMSSender {

  private String accoutReference;

  private String password;

  private SMSSender smsSender;

  private String username;

  @Activate
  public void activate() {
    smsSender = new EsendexSMSSender(username, password, accoutReference);
  }

  @Override
  public void sendSMS(final String recipientNumber, final String message) {
    smsSender.sendSMS(recipientNumber, message);
  }

  @StringAttribute(defaultValue = "")
  public void setAccoutReference(final String accoutReference) {
    this.accoutReference = accoutReference;
  }

  @StringAttribute(defaultValue = "")
  public void setPassword(final String password) {
    this.password = password;
  }

  @StringAttribute(defaultValue = "")
  public void setUsername(final String username) {
    this.username = username;
  }

}