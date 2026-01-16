function login() {
	alert('ccccc');
	var searchName = $("#account").val();
	var searchNameFmt = cleanText(searchName);
	if (searchNameFmt != null) {
		$("#account").val(searchNameFmt);
	}

	$("#loginForm").attr("action", './login');
	$("#loginForm").submit();

	// 清空
	$("#account").attr("value", '');
	$("#password").attr("value", '');
	$("#loginForm").attr("action", '');
}

/*function searchComic() {
	var searchName = $("#searchName").val();
	var searchNameFmt = cleanText(searchName);
	if (searchNameFmt != null) {
		$("#searchName").val(searchNameFmt);
	}

	$("#searchForm").attr("action", './list_comics.html');
	$("#searchForm").submit();

	// 清空
	$("#searchName").attr("value", '');
	$("#searchForm").attr("action", '');
}

function onSearch(keyword) {
	if (keyword == null || keyword == '' || keyword == undefined) {
		return;
	}

	$("#searchName").attr("value", keyword);
	searchVideo();
}

// 头动画
function showHeadGif() {
	var gifImgs = [];
	gifImgs[0] = "./img/gif/as_1.gif";
	gifImgs[1] = "./img/gif/as_2.gif";
	gifImgs[2] = "./img/gif/as_3.gif";
	gifImgs[3] = "./img/gif/flk_1.gif";
	gifImgs[4] = "./img/gif/j3_1.gif";
	var randomBgIndex = Math.round(Math.random() * 4);
	var img = document.getElementById('onepieceGif');
	img.setAttribute("src", gifImgs[randomBgIndex]);
}
window.setInterval(function() {
	showHeadGif()
}, 10000);*/

// Scroll page to the bottom
// $(document).ready(function() {
// $('html, body').animate({
// scrollTop : $(document).height()
// }, 'slow');
// });

// ////////////////////////////