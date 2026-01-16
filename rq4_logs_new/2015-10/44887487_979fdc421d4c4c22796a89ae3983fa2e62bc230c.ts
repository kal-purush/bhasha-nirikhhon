

import WebContainerLayout = require("si-portal-framework/siPortal/rootLayouts/webContainerLayout");
import SIStackLayout = require("si-portal-framework/siPortal/stackLayout/siStackLayout");
import SIStackLayoutOrientation = require("si-portal-framework/siPortal/stackLayout/siStackLayoutOrientation");  
import AzurePortalSideBarLayout = require("./sidebar/AzurePortalSideBarLayout");

class AzurePortalLayout extends WebContainerLayout {
    constructor(opt?) {
        super({
            layout: new SIStackLayout({
                classes : ["portal-main"],
                orientation: SIStackLayoutOrientation.horizontal,
                elements: [new AzurePortalSideBarLayout(
                    {
                        collapsed: false,
                        favorites: {
                            favorites: [
                                { opensExternal: true, label: "Test 1", uri: "#/Test1" },
                                { opensExternal: true, label: "Test 2", uri: "#/Test2" },
                                { opensExternal: true, label: "Test 3", uri: "#/Test3" },
                                { opensExternal: true, label: "Test 4", uri: "#/Test4" },
                                { opensExternal: true, label: "Test 5", uri: "#/Test5" },
                            ]
                        }
                    }
                )]
            })            
        });           
    }

}

export =  AzurePortalLayout;