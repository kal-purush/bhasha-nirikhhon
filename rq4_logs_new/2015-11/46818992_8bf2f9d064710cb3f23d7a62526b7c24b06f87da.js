emojify.setConfig({
	img_dir: '/plugins/emoji/assets/images',
	blacklist: {
		elements: ['script', 'textarea', 'a', 'pre', 'code'],
		classes: ['nosmile', 'no-emojify'],
	},
});
emojify.run();