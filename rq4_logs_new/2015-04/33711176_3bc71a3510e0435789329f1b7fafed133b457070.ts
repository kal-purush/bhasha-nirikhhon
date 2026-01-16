/**
 * New typescript file
 */
class AccessLoggerItems {
	thisUserAgent:String = window.navigator.userAgent;
	thisURL:String = window.location.href;
	thisUser:String = "";
	thisIPAddress:String = "";
	thisTargetDOM = "";
	thisAction:String = "";
	function SendAction(e : MouseEvent) {
		alert("a");
	}
}


class AccessLogger extends AccessLoggerItems {
	/*このライブラリが対応するすべての要素にアクセスロガーを仕込む*/
	SetLoggerToThisPage() {
		alert(document.getElementsByTagName("*"));
	}
	/*読み込んだページのinput type="button"にアクセスロガーを設定*/
	SetLoggerToButtonThisPage() {
		var ButtonTags = document.getElementsByTagName("input");
		var targetObj : HTMLElement;
		for(var i = 0; i < ButtonTags.length; i++) {
			alert(ButtonTags[i].id + ":" + ButtonTags[i].type);

			if (ButtonTags[i].type.toString()  == "button" ) {
				targetObj = document.getElementById(ButtonTags[i].id.toString());
				targetObj.addEventListener("click", SendAction, false);
			}
		}
	}


}


AccessLogger.prototype.SetLoggerToButtonThisPage();