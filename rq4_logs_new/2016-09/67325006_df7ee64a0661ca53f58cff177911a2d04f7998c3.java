/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xj.apps.controllers;

import xj.core.controllers.CoreActionBean;
import net.sourceforge.stripes.action.DefaultHandler;
import net.sourceforge.stripes.action.ForwardResolution;
import net.sourceforge.stripes.action.Resolution;
import net.sourceforge.stripes.action.UrlBinding;

/**
 *
 * @author anto89
 */
@UrlBinding("/home.action")
public class HomeActionBean extends CoreActionBean {
    
    @DefaultHandler
    public Resolution home() {
        // TODO 
        return new ForwardResolution("xj.apps/modules/home/views/home.jsp");
    }
}