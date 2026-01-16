class HomeMenuItem {
	beforeRegister() {
		this.is = 'home-menu-item';
		this.properties = {
			content: {
				type: String
			}
		};
	}

	created() { }
	ready() { }
	attached() { }
	detached() { }
	attributeChanged() { }
}

Polymer(HomeMenuItem);