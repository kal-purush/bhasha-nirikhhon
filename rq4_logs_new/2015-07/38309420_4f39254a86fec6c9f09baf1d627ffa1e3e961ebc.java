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

/**
 * Service properties of the {@link org.everit.sms.esendex.ecm.internal.EsendexSMSSenderComponent}.
 */
public final class EsendexSMSSenderComponentConfig {

  public static final String ATTR_ACCOUNT_REFERENCE = "accoutReference";

  public static final String ATTR_PASSWORD = "password";

  public static final String ATTR_SMS_PROVIDER = "sms.provider";

  public static final String ATTR_USERNAME = "username";

  public static final String COMPONENT_ID = "org.everit.sms.esendex.ecm.EsendexSMSSenderComponent";

  public static final String SMS_PROVIDER_ESENDEX = "esendex";

  private EsendexSMSSenderComponentConfig() {
  }

}