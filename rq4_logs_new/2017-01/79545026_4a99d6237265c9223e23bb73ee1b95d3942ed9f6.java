/*
 * ProActive Parallel Suite(TM):
 * The Open Source library for parallel and distributed
 * Workflows & Scheduling, Orchestration, Cloud Automation
 * and Big Data Analysis on Enterprise Grids & Clouds.
 *
 * Copyright (c) 2007 - 2017 ActiveEon
 * Contact: contact@activeeon.com
 *
 * This library is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation: version 3 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 */
package org.ow2.proactive.addons.ldap_query;/*
                                            * ProActive Parallel Suite(TM):
                                            * The Open Source library for parallel and distributed
                                            * Workflows & Scheduling, Orchestration, Cloud Automation
                                            * and Big Data Analysis on Enterprise Grids & Clouds.
                                            *
                                            * Copyright (c) 2007 - 2017 ActiveEon
                                            * Contact: contact@activeeon.com
                                            *
                                            * This library is free software: you can redistribute it and/or
                                            * modify it under the terms of the GNU Affero General Public License
                                            * as published by the Free Software Foundation: version 3 of
                                            * the License.
                                            *
                                            * This program is distributed in the hope that it will be useful,
                                            * but WITHOUT ANY WARRANTY; without even the implied warranty of
                                            * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
                                            * GNU Affero General Public License for more details.
                                            *
                                            * You should have received a copy of the GNU Affero General Public License
                                            * along with this program. If not, see <http://www.gnu.org/licenses/>.
                                            *
                                            * If needed, contact us to obtain a release under GPL Version 2 or 3
                                            * or a different license than the AGPL.
                                            */

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.ow2.proactive.addons.ldap_query.model.ErrorResponse;
import org.ow2.proactive.addons.ldap_query.model.LDAPResponse;
import org.ow2.proactive.addons.ldap_query.model.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class LDAPClient {
    private final static String LDAP_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    private final static String SECURITY_AUTHENTICATION_METHOD = "simple";

    private static DirContext connect(String ldapServerUrl, String username, String password) throws NamingException {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_FACTORY);
        env.put(Context.PROVIDER_URL, ldapServerUrl);
        env.put(Context.SECURITY_AUTHENTICATION, SECURITY_AUTHENTICATION_METHOD);
        env.put(Context.SECURITY_CREDENTIALS, password);
        env.put(Context.SECURITY_PRINCIPAL, username);
        return new InitialDirContext(env);
    }

    public static String searchQueryLDAP(String ldapServerUrl, String username, String password, String searchBase,
            String searchFilter) {
        DirContext ctx = null;
        NamingEnumeration results = null;
        ObjectMapper mapper = new ObjectMapper();
        Response response;
        List attributesList = new LinkedList();
        String resultOutput = "";
        try {
            ctx = connect(ldapServerUrl, username, password);
            SearchControls controls = new SearchControls();
            controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            results = ctx.search(searchBase, searchFilter, controls);

            //iterate throw all attributes in the result of search query
            while (results.hasMore()) {
                SearchResult searchResult = (SearchResult) results.next();
                Attributes attributes = searchResult.getAttributes();
                Map<String, String> attrMap = new HashMap();
                for (NamingEnumeration ae = attributes.getAll(); ae.hasMore();) {
                    Attribute attr = (Attribute) ae.next();
                    attrMap.put(attr.getID(), attr.get().toString());
                }
                attributesList.add(attrMap);
            }
            response = new LDAPResponse("Ok", attributesList);
        } catch (Exception e) {
            response = new ErrorResponse("Error", e.toString());
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception e) {
                }
            }
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception e) {
                }
            }
        }
        try {
            resultOutput = mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return resultOutput;
    }

    public static void main(String[] args) {
        String searchBase = "cn=yaro,dc=activeeon,dc=com1";
        String searchFilter = "(objectclass=*)";

        System.out.println(searchQueryLDAP("ldap://192.168.1.136:389/",
                                           "cn=admin,dc=activeeon,dc=com",
                                           "activeeon",
                                           searchBase,
                                           searchFilter));
    }
}