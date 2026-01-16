const redirect_uri = 'http://127.0.0.1:5500/';

const AUTHOURIZE = 'https://accounts.spotify.com/authorize?';
const TOKEN = 'https://accounts.spotify.com/api/token?';

const RECENT = 'https://api.spotify.com/v1/me/player/recently-played';
const ARTISTS = 'https://api.spotify.com/v1/me/top/artists';
const TRACKS = 'https://api.spotify.com/v1/me/top/tracks';

const PLAYLISTS = 'https://api.spotify.com/v1/me/playlists';
const USER = 'https://api.spotify.com/v1/me';
const ID = 'https://api.spotify.com/v1';
const SAVED = 'https://api.spotify.com/v1/me/tracks/contains';

document.addEventListener('DOMContentLoaded', (e) => onload_events())

const init = async () => {
	let credentials = await fetch('credentials.json');
	let response = await credentials.json();
	return response;
}

function onload_events() {
	get_res();
	if (window.location.search.length > 0) {
		handle_redirect();
	}
}

async function request_auth() {
	console.log('auth');

	let scope = 'user-read-private user-top-read user-read-playback-state user-top-read user-library-read user-read-recently-played';

	let clientdata = await init();

	let url = AUTHOURIZE
		+ 'client_id=' + clientdata.client_id
		+ '&response_type=code'
		+ '&redirect_uri=' + encodeURI(redirect_uri)
		// + '&show_dialog=true'
		+ '&scope=' + scope;

	window.location.href = url;
}

function handle_redirect() {

	console.log('redirect');

	let query_string = window.location.search;
	let url_params = new URLSearchParams(query_string);
	let code = url_params.get('code');

	fetch_token(code);
	window.history.pushState("", "", redirect_uri);

}
let access_token = ''
async function fetch_token(code) {

	let url = TOKEN
		+ 'grant_type=authorization_code'
		+ '&code=' + code
		+ '&redirect_uri=' + encodeURI(redirect_uri);

	let clientdata = await init();

	fetch(url, {
		method: 'POST',
		headers: {
			'Authorization': 'Basic ' + btoa(clientdata.client_id + ':' + clientdata.client_secret),
			'Content-Type': 'application/x-www-form-urlencoded'
		}
	})
		.then(response => response.json())
		.then(data => {
			access_token = data.access_token;
			show_elements();
			hide_startbtn();
			show_profile(access_token);
			show_items(access_token, selection)
		});

}

async function show_profile(access_token) {
	const data = await call_api(USER, access_token);

	console.log(data)

	const profile = document.querySelector('.profile');
	let profile_img = document.createElement('img');
	let profile_name = document.createElement('div');

	profile_name.innerHTML = data.display_name;
	profile_img.src = data.images[0].url;

	profile_name.className = 'profilename';

	profile.appendChild(profile_img);
	profile.appendChild(profile_name);

}

const show_elements = () => {
	document.querySelector('.title').style.display = 'block';
	document.querySelector('.intro').style.display = 'none';
	menu.style.display = 'block';
	submenu.style.display = 'block';
};
const hide_startbtn = () => document.getElementById('startbtn').style.display = 'none';

const menucontainer = document.querySelector('.menucontainer');
const menu = document.querySelector('.menu');
const submenu = document.querySelector('.submenu');
const gallery = document.querySelector('.gallery')

let time_range = 'short_term',
	selection = 'toptracks',
	disable_submenu = false;

function toggle_active() {
	let label = document.querySelectorAll('.submenuelem>input+label');
	let radio = document.querySelectorAll('.submenuelem>input');
	label.forEach(elem => elem.classList.toggle('inactive'));
	radio.forEach(
		elem => elem.disabled = (!elem.disabled)
	)

}

menucontainer.addEventListener('click', (e) => {

	if (e.target.name == 'select') {
		if (gallery.children.length > 0)
			clearchildren(gallery);

		selection = e.target.id;

		if (disable_submenu != (selection == 'recents')) {
			toggle_active();
		} disable_submenu = (selection == 'recents');

		show_items(access_token, selection);
	}

	else if (e.target.name == 'timerange') {
		time_range = e.target.id;

		if (gallery.children.length > 0)
			clearchildren(gallery);

		show_items(access_token, selection)
	}
})



async function call_api(url, access_token) {
	return fetch(url, {
		headers: {
			'Authorization': "Bearer " + access_token,
			"Content-Type": "application/json",
			"Accept": "application/json"
		}
	})
		.then(response => response.json())
		.then(data => {
			return data;
		});
}



async function show_items(access_token, type) {
	let url, src, heading, subheading = null;

	if (type == 'recents')
		url = RECENT + '?limit=50';
	else if (type == 'topartists')
		url = ARTISTS + `?time_range=${time_range}&limit=50`;
	else if (type == 'toptracks')
		url = TRACKS + `?time_range=${time_range}&limit=50`;

	const data = await call_api(url, access_token);

	for (i in data.items) {
		let n = parseInt(i) + 1;

		if (type == 'recents') {
			subheading = data.items[i].track.artists;
			subheading = subheading.map(x => x.name).join(', ');
			src = data.items[i].track.album.images[1].url;
			heading = data.items[i].track.name;
		}
		else if (type == 'topartists') {
			src = data.items[i].images[1].url;
			heading = data.items[i].name;
		}
		else if (type == 'toptracks') {
			src = data.items[i].album.images[1].url;
			heading = data.items[i].name;
			subheading = data.items[i].artists;
			subheading = subheading.map(x => x.name).join(', ');
		}
		add_item(n, src, heading, subheading, gallery)
	}
}

function add_item(n, src, heading, subheading, node) {

	let item = document.createElement('div');
	item.className = 'item';
	node.appendChild(item);

	let card = document.createElement('div');
	card.className = 'card';
	card.innerHTML = n;
	item.appendChild(card);

	let img = document.createElement('img');
	img.src = src;
	item.appendChild(img);

	let item_head = document.createElement('div');
	item_head.className = 'item-head';
	item_head.innerHTML = truncate(heading, 30);
	item.appendChild(item_head);

	if (subheading) {
		let item_subhead = document.createElement('div');
		item_subhead.className = 'item-subhead';
		item_subhead.innerHTML = truncate(subheading, 40);
		item.appendChild(item_subhead);
	}

}


const clearchildren = (parent) => {
	while (parent.firstChild) {
		parent.removeChild(parent.lastChild);
	}
}

const get_res = () => {
	var ratio = window.devicePixelRatio || 1;
	var w = screen.width * ratio;
	var h = screen.height * ratio;

	console.log(w + ' ' + h)
}

const truncate = (str, n) => {
	if (str == null)
		return;
	let x = (str.length > n) ? str.substr(0, n - 1) + '&hellip;' : str;
	return x;
};