function executeWidgetCode() {
    require(["DS/WAFData/WAFData", "DS/i3DXCompassServices/i3DXCompassServices"], function(WAFData, i3DXCompassServices) {
        var myWidget = {
			varServiceURL: "",
            dataFull: [],
            title:"",
            state:"",
            project:"",
            

            displayData: function(arrData) {
                var tableHTML =
                    "<div style='height:100%;overflow:auto;'><table><thead><tr><th>Title</th><th>State</th><th>Policy</th><tbody>";

                for (var i = 0; i < arrData.length; i++) {
                    tableHTML =
                        tableHTML +
                        "<tr><td>" +
                        arrData[i].title +
                        "</td><td>" +
                        arrData[i].state +
                        "</td><td>" +
                        arrData[i].policy +
                        "</td></tr>" 
                       
                }

                tableHTML += "</tbody></table></div>";

                widget.body.innerHTML = tableHTML;
            },

            onLoadWidget: function() {
                myWidget.callData();
                myWidget.displayData(myWidget.dataFull);
            },

            callData: function() {
				i3DXCompassServices.getServiceUrl({
				    serviceName: '3DSpace', 
				    platformId: widget.getValue('x3dPlatformId'),
				    onComplete : function (URLResult){
						myWidget.tableData(URLResult);
					},
					onFailure : function (error){
						console.log(error);
					}
				});				
			},
			
			tableData: function(serviceURL) {
				var urlWAF = serviceURL + "/resources/v1/modeler/tasks?tenant=r1132103242220";
				console.log(urlWAF);
                var methodWAF = "GET";
                WAFData.proxifiedRequest(urlWAF, {
                    proxy : 'passport' ,
					type: "json",
					onComplete: function(dataResp) {
                        const tasks=[];
							dataResp.data.forEach(element => {
                                tasks.push({
                                    title : element.dataelements.title,
                                    state:element.dataelements.state,
                                    policy:element.dataelements.policy
								})
                            })
                            myWidget.dataFull=tasks;
                            console.log("datafull",myWidget.dataFull);
                            myWidget.displayData(myWidget.dataFull);
					},
					onFailure: function(error) {
						widget.body.innerHTML += "<p>Call Faillure</p>";
						widget.body.innerHTML += "<p>" + JSON.stringify(error) + "</p>";
					}
                });
             }
        };

        widget.addEvent("onLoad", myWidget.onLoadWidget);
        widget.addEvent("onRefresh", myWidget.onLoadWidget);
        
    });
}