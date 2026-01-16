function BannersItem(params) {
	var params = params || {},
		href = params.href || '#',
		type = +(params.type || 0),
		src,
		alt = params.alt || 'alt',
		button = !!params.button ? '<a class="b-button nav-banners-button" href="' + href + '">' + params.button + '</a>' : '';


	if(type === 1 || type === 2) {
		width = 250;
		height = 160;
		type = 'nav-banners__item--type-' + type;
		src = params.src || 'http://placehold.it/250x160';
	}
	else if(type === 3 || type === 4) {
		width = 250;
		height = 330
		type = 'nav-banners__item--type-' + type;
		src = params.src || 'http://placehold.it/250x330';
	}
	else if(type === 5) {
		width = 290;
		height = 130;
		type = 'nav-banners__item--type-' + type;
		src = params.src || 'http://placehold.it/290x130';
	}

	var link = '<a class="nav-banners-link" href=""><img class="nav-banners-link__img" src="' + src + '" width="' + width + '" height="' + height + '" alt="' + alt + '"></a>';


	return '<div class="nav-banners__item  ' + type + '">' + link + button + '</div>';
}


function navBannersListInit(listId, listType, banners) {

}

var navBanners = {
		id: 1,
		'Engagement Rings': function () {
			return '<div class="nav-banners  nav-banners--type-1">' 
				+ BannersItem({ type: 1, src: 'https://jira.zoloto585.ru/secure/attachment/13670/rings_%21.jpg', button: 'Другая кнопка'}) 
				+ BannersItem({ type: 2, src: 'https://jira.zoloto585.ru/secure/attachment/13671/rings_2.jpg', }) 
				+ BannersItem({ type: 4, src: 'https://jira.zoloto585.ru/secure/attachment/13669/ring_3.jpg', button: 'Кольца' }) 
				+ '</div>';
		},

		'Silver': function () {
			return '<div class="nav-banners  nav-banners--type-3">' 
				+ BannersItem({ type: 5, src: 'https://jira.zoloto585.ru/secure/attachment/13670/rings_%21.jpg', button: 'Другая кнопка'}) 
				+ BannersItem({ type: 5, src: 'https://jira.zoloto585.ru/secure/attachment/13671/rings_2.jpg', }) 
				+ BannersItem({ type: 5, src: 'https://jira.zoloto585.ru/secure/attachment/13669/ring_3.jpg', button: 'Кольца' }) 
				+ BannersItem({ type: 5, src: 'https://jira.zoloto585.ru/secure/attachment/13669/ring_3.jpg', button: 'Кольца' }) 
				+ '</div>';
		},

		'Watch': function () {
			return '<div class="nav-banners  nav-banners--type-1">' 
				+ BannersItem({ type: 3, src: 'https://jira.zoloto585.ru/secure/attachment/13670/rings_%21.jpg', button: 'Мужские'}) 
				+ BannersItem({ type: 4, src: 'https://jira.zoloto585.ru/secure/attachment/13671/rings_2.jpg', button: 'Женские' }) 
				+ '</div>';
		},

		'WWatch': {
			
		},

		init: function (id, banners) {
			document.querySelector('.main-nav__item[data-inner-list="' + id + '"]').innerHTML += banners;
		},
	};





/*document.querySelector('.main-nav__item[data-inner-list="1"]').innerHTML += navBanners['Engagement Rings']();
document.querySelector('.main-nav__item[data-inner-list="7"]').innerHTML += navBanners['Silver']();
document.querySelector('.main-nav__item[data-inner-list="6"]').innerHTML += navBanners['Watch']();*/

navBanners.init(6, navBanners['Watch']());