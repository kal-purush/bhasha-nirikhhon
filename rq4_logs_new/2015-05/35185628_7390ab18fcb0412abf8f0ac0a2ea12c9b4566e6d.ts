import {Utils} from './Utils';

export class Renderer {
	private container;

	private renderImage(image) {
		var img = document.createElement('img');
		img.src = 'http://media8.clubpenguin.com/mobile/cp-mobile-ui/clubpenguin_v1_6/en_US/deploy/metaplace/devicepng/assets/' + image.image;
		img.style.position = 'absolute';
		img.style.left = image.originX + 'px';
		img.style.top = image.originY + 'px';
		this.container.appendChild(img);
	}

	private renderButton(button) {
		var img = document.createElement('img');
		img.src = 'http://media8.clubpenguin.com/mobile/cp-mobile-ui/clubpenguin_v1_6/en_US/deploy/metaplace/devicepng/assets/' + button.imageUp;
		img.style.position = 'absolute';
		img.style.left = button.originX + 'px';
		img.style.top = button.originY + 'px';
		this.container.appendChild(img);
	}

	private renderComponent(component) {
		if (component.images) {
			for (let image of component.images) {
				this.renderImage(image);
			}
		}
		if (component.buttons) {
			for (let button of component.buttons) {
				this.renderButton(button);
			}
		}
	}

	public constructor(container) {
		this.container = container;

		Utils.getFile('http://media8.clubpenguin.com/mobile/cp-mobile-ui/clubpenguin_v1_6/en_US/deploy/metaplace/devicepng/config/catalog.json', function(content) {
			let data = JSON.parse(content);

			container.style.width = data.sourceWidth + 'px';
			container.style.height = data.sourceHeight + 'px';
			container.style.position = 'relative';
			container.style.backgroundColor = 'red';

			for (let component of data.components) {
				if (component.name === 'purchase_dialog') {
					continue;
				}
				this.renderComponent(component.layout);
			}

			Utils.getFile('http://media8.clubpenguin.com/mobile/cp-mobile-ui/clubpenguin_v1_6/en_US/deploy/metaplace/devicepng/config/catalog/penstyle.json', function(content) {
				let data = JSON.parse(content);

				this.renderComponent(data.dynamic);

				for (let component of data.components) {
					this.renderComponent(component.layout);
				}
			}.bind(this));
		}.bind(this));
	}
}