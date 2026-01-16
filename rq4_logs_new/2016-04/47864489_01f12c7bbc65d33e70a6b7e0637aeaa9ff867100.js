/*
 *
 *   Main 
 *   version 1.0
 *    2016-04-15 Fri
 */

/* 전체 선택 */
function checkAll(formname){
	var df = document[formname];
	for(var i=0;i<df.elements.length;i++){
		if(df[i].type=="checkbox"){
//			(df[i].checked == true)?df[i].checked = false:df[i].checked = true;
			df[i].checked = !df[i].checked;

		}
	}
}