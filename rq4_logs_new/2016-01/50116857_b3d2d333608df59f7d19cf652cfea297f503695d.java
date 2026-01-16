/**
 * Copyright (C) 2016 Rivet Logic Corporation.
 * 
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; version 3 of the License.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses/>.
 */

package com.rivetlogic.ecommerce.configuration;

import com.liferay.portal.kernel.portlet.DefaultConfigurationAction;
import com.liferay.portal.kernel.util.Constants;
import com.liferay.portal.kernel.util.ParamUtil;
import com.liferay.util.portlet.PortletProps;
import com.rivetlogic.ecommerce.beans.ShoppingCartPrefsBean;
import com.rivetlogic.ecommerce.util.PreferencesKeys;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.portlet.ActionRequest;
import javax.portlet.ActionResponse;
import javax.portlet.PortletConfig;
import javax.portlet.PortletPreferences;
import javax.portlet.ReadOnlyException;
import javax.portlet.RenderRequest;
import javax.portlet.RenderResponse;
import javax.portlet.ValidatorException;

/**
 * @author isaiulate
 */

public class PortletConfiguration extends DefaultConfigurationAction{
	
	@Override
	public String render(PortletConfig portletConfig, RenderRequest renderRequest, RenderResponse renderResponse) throws Exception{
		checkPorletConfiguration(renderRequest);
		renderRequest.setAttribute("portletConfig", new ShoppingCartPrefsBean(renderRequest));
		return super.render(portletConfig, renderRequest, renderResponse);
	}
	
	public static void checkPorletConfiguration(RenderRequest renderRequest) throws ReadOnlyException, ValidatorException, IOException{
		PortletPreferences portletPreferences = renderRequest.getPreferences();
		if(null == portletPreferences ||
				!Boolean.toString(true).equals(portletPreferences.getValue(PreferencesKeys.CHANGED, Boolean.toString(false)))){
			Map <String, String> defaultPropsMap = loadDefaultConfig();
			if(null != defaultPropsMap){
				for(Entry<String, String>entry : defaultPropsMap.entrySet()){
					portletPreferences.setValue(entry.getKey(), entry.getValue());
				}
				portletPreferences.store();
			}
		}
	}
	
	@Override
	public void processAction(
	    PortletConfig portletConfig, ActionRequest actionRequest,
	    ActionResponse actionResponse) throws Exception {  
	    super.processAction(portletConfig, actionRequest, actionResponse);
	    if(Constants.UPDATE.equals(ParamUtil.getString(actionRequest, Constants.CMD))){
	    	PortletPreferences prefs = actionRequest.getPreferences();
	    	prefs.setValue(PreferencesKeys.CHANGED, Boolean.toString(true));
	    	prefs.store();
	    }
	}
	
	private static Map <String, String> loadDefaultConfig(){
		Map <String, String> defaultPropsMap = new HashMap<String, String>();
		for(String defaultPropertyKey : defaultValuesProps){
			String propertyValue = PortletProps.get(defaultPropertyKey);
			if(null != propertyValue){
				System.out.println("Key: "+defaultPropertyKey +" (translated: "+ translateKey(defaultPropertyKey) +") "+ " , value: "+propertyValue);
				defaultPropsMap.put(translateKey(defaultPropertyKey), propertyValue);
			}
		}
		return defaultPropsMap;
	}
	
	private static final String translateKey(String key){
		switch(key){
			case PreferencesKeys.DEFAULT_STORE_EMAIL_SUBJECT:
				return PreferencesKeys.STORE_NOTIF_SUBJECT_TEMPLATE;
			case PreferencesKeys.DEFAULT_STORE_EMAIL_BODY:
				return PreferencesKeys.STORE_NOTIF_BODY_TEMPLATE;
			case PreferencesKeys.DEFAULT_CUSTOMER_EMAIL_SUBJECT:
				return PreferencesKeys.CUSTOMER_NOTIF_SUBJECT_TEMPLATE;
			case PreferencesKeys.DEFAULT_CUSTOMER_EMAIL_BODY:
				return PreferencesKeys.CUSTOMER_NOTIF_BODY_TEMPLATE;
			case PreferencesKeys.DEFAULT_MESSAGE_CART_EMPTY:
				return PreferencesKeys.CART_EMPTY_MESSAGE;
			case PreferencesKeys.DEFAULT_MESSAGE_CHECKOUT_SUCCESS:
				return PreferencesKeys.CHECKOUT_SUCCESS_MESSAGE;
			case PreferencesKeys.DEFAULT_MESSAGE_CHECKOUT_ERROR:
				return PreferencesKeys.CHECKOUT_ERROR_MESSAGE;
			default:
				return key;
		}
	}
	
	private static final String[] defaultValuesProps = {PreferencesKeys.DEFAULT_STORE_EMAIL_SUBJECT, 
		PreferencesKeys.DEFAULT_STORE_EMAIL_BODY, PreferencesKeys.DEFAULT_CUSTOMER_EMAIL_SUBJECT,
		PreferencesKeys.DEFAULT_CUSTOMER_EMAIL_BODY, PreferencesKeys.DEFAULT_MESSAGE_CART_EMPTY, 
		PreferencesKeys.DEFAULT_MESSAGE_CHECKOUT_SUCCESS, PreferencesKeys.DEFAULT_MESSAGE_CHECKOUT_ERROR};
}