var BASE_URL = 'https://rest-iuts.pkkmart.com/';

function gettoken(argument) {
	$.ajax({
		url: BASE_URL + 'ApiController/cektoken',
		type: 'GET',
		dataType: 'json',
		success:function(data) {
			$("#token").val(data);
		}
	})
}
function login() {
	$.ajax({
		url: BASE_URL + 'ApiController/loginname',
		type: 'POST',
		dataType: 'json',
		data: {email: $("#email").val(),password:$("#password").val(),token:$("#token").val()},
		success:function(data) {
			window.location.href = 'http://jakevo.jakarta.go.id/pemohon#/pengajuan-pencarian';
		}
	})
}