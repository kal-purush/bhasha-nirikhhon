import api from './api';

export default {
	default: {
		title: 'HarrisonFM',
		tagline: 'Adventure, Photography, and Coding',
		desc: 'Harrison June is an American adventure photographer, writer and coder based in Arizona.',
		img: 'img.png',
		url: 'https://harrisonfm.com'
	},
	setDefaults: function() {
		api.getSitemeta((response) => {
			if(response.title) {
				this.default.title = response.title;
			}
			if(response.tagline) {
				this.default.tagline = response.tagline;
			}
			if(response.img) {
				this.default.img = response.img[0];
			}
			if(response.url) {
				this.default.url = response.url;
			}
		});
	},
	setMeta: function(title, desc, img, url) {
		Array.from(document.querySelectorAll('[data-vue-meta]')).map(el => el.parentNode.removeChild(el));

		document.title = title ? title + ' - ' + this.default.title : this.default.title + ' - ' + this.default.tagline;

		desc = desc ? desc : this.default.desc;
		img = img ? img : this.default.img;
		url = url ? url : this.default.url;

		const tags = [
			{
				attribute: 'name',
				attributeContent: 'description',
				content: desc
			},
			{
				attribute: 'name',
				attributeContent: 'image',
				content: img
			},
			{
				attribute: 'property',
				attributeContent: 'og:title',
				content: title ? title : this.default.title + ' - ' + this.default.tagline
			},
			{
				attribute: 'property',
				attributeContent: 'og:description',
				content: desc
			},
			{
				attribute: 'property',
				attributeContent: 'og:url',
				content: url
			},
			{
				attribute: 'name',
				attributeContent: 'og:image',
				content: img
			},
			{
				attribute: 'property',
				attributeContent: 'twitter:title',
				content: title ? title : this.default.title + ' - ' + this.default.tagline
			},
			{
				attribute: 'property',
				attributeContent: 'twitter:description',
				content: desc
			},
			{
				attribute: 'property',
				attributeContent: 'twitter:url',
				content: url
			},
			{
				attribute: 'name',
				attributeContent: 'twitter:image',
				content: img
			},
		];

		for (const i in tags) {
			const tag = document.createElement('meta');
			tag.setAttribute(tags[i].attribute, tags[i].attributeContent);
			tag.setAttribute('content', tags[i].content);
			tag.setAttribute('data-vue-meta', '');
			document.head.appendChild(tag);
		}
	}
};