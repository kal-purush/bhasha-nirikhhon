function getXHR(){
	  if( window.XMLHttpRequest ){
	  	return new XMLHttpRequest();
	  }else{
	  	return new ActiveXObject('Microsoft.XMLHTTP');
	  }
}

var QData = {
	QBox: null,
	PBox: null,
	CBox: null,
	ABox: null,
	SetQuestion:  function(q){
		this.QBox.value = q;
	},
	SetPoints: function(p){
		this.PBox.value = p;
	},
	SetCategory: function(c){
		this.CBox.value = c;
	},
	SetAnswer: function(a){
		this.ABox.value = a;
	},
	ShowQuestionDialog: function(){
		Data.ShowDialog('Question');
	},
	
	ID: -1,
	action: "modify"
};

function create_question(){
	QData.SetQuestion("");
	QData.SetPoints(0);
	QData.SetCategory("");
	QData.SetAnswer("");
	QData.ID = -1;
	QData.action = "create";
	
	QData.ShowQuestionDialog();
}

function load_question(id){
	var xhr = getXHR();
	
	xhr.onreadystatechange = function(){
		if( xhr.readyState == 4 && xhr.status == 200 ){
			var data = JSON.parse(xhr.responseText);
			QData.SetQuestion( data.qtext );
			QData.SetPoints( data.points );
			QData.SetCategory( data.category );
			QData.SetAnswer( data.answer );
			QData.ID = id;
			QData.ShowQuestionDialog();
		}else{
			if( xhr.readyState == 4 && xhr.status != 200 ){
				Data.SetFailureMessage( "A server error occurred." );
				Data.ShowDialog('Failure');
			}
		}
	};
	
	QData.action="modify";
	
	xhr.open( 'POST', 'php-bin/challenge_api.php' );
	xhr.setRequestHeader( 'Content-type', 'application/x-www-form-urlencoded' );
	xhr.send( 'action=fetch_full&id=' + encodeURIComponent(id) );
}

function set_question(evt){
	if( evt.preventDefault ){
		evt.preventDefault();
	}else if( evt.stopPropagation ){
		evt.stopPropagation();
	}else{
		evt.cancelBubble = true;
	}
	
	var xhr = getXHR();
	
	xhr.onreadystatechange = function(){
		if( xhr.readyState == 4 && xhr.status == 200 ){
			if(xhr.responseText == 'OK'){
				Data.SetSuccessMessage('Updated!');
				Data.ShowDialog( 'Success' );
			}else{
				Data.SetFailureMessage( xhr.responseText );
				Data.ShowDialog( 'Failure' );
			}
		}else{
			if( xhr.readyState == 4 && xhr.status != 200 ){
				Data.SetFailureMessage( "A server error occurred." );
				Data.ShowDialog('Failure');
			}
		}
	};
	
	xhr.open( 'POST', 'php-bin/challenge_api.php' );
	xhr.setRequestHeader( 'Content-type', 'application/x-www-form-urlencoded' );
	var ans = document.forms[2].elements['answer'].value;
	
	var datael = document.getElementById('question' + QData.ID);
	if( isNaN(parseInt(QData.PBox.value)) ){
		Data.SetFailureMessage('Points must be a number!');
		Data.ShowDialog('Failure');
		return;
	}
	//Even if QData.action is modified with malicious intent, it cannot do any harm.
	//It is not injected into a SQL query, and even if it was, then it would still
	//not do any harm, because every value injected into any query is escaped using
	//mysqli::real_escape_string.
	xhr.send( 'action=' + encodeURIComponent(QData.action) + '&id=' + encodeURIComponent(QData.ID) + '&a=' + encodeURIComponent(ans) + '&q=' + encodeURIComponent(QData.QBox.value) + '&cat=' + encodeURIComponent(QData.CBox.value) + '&points=' + encodeURIComponent(QData.PBox.value) );
}

function InitChallenges(){
	QData.QBox = document.forms[2].elements['question'];
	QData.PBox = document.forms[2].elements['points'];
	QData.CBox = document.forms[2].elements['category'];
	QData.ABox = document.forms[2].elements['answer'];
}

addEventListener('load', InitChallenges);