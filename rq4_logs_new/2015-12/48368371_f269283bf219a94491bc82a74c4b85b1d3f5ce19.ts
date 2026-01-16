/// <reference path='node_modules/typescript-collections/collections.ts' />
/// <reference path="typings/jquery/jquery.d.ts" />

/**
 * Main module
 */
module CafeApp {

	class Order {
		constructor(public id: string, public name: string){
			//
		}
	}

	export class Cafe {
		private waiter = new Worker('waiter.js');
		private chef = new Worker('chef.js');

		constructor() {
			var queue = new collections.Queue<Order>();
			var app = this;

			this.waiter.onmessage = (event) => {
				console.log('Cafe: received event from Waiter worker: ' + JSON.stringify(event.data));
				console.log('Cafe: queue.size(), v1: ' + queue.size());

				switch(event.data['messageName']) {
					case 'addOrderToQueueCommand':
						var order: Order = JSON.parse(event.data.payload);
						queue.add(order);
						this.displayQueue(queue);
					break;
				}

				console.log('Cafe: queue.size(), v2: ' + queue.size());
			};

			this.chef.onmessage = (event) => {
				console.log('Cafe: received event from Chef worker: ' + JSON.stringify(event.data['messageName']));
				switch(event.data['messageName']) {
					case 'askForWorkCommand':
						var nextOrderToCook:Order = queue.dequeue();
						this.displayQueue(queue);
						if (nextOrderToCook !== null && nextOrderToCook !== undefined && nextOrderToCook.id !== null) {
							console.log("Cafe: dequeue one item: " + JSON.stringify(nextOrderToCook));
							this.chef.postMessage({'messageName': 'cookCommand', 'payload': nextOrderToCook});
							this.displayCookingItem(nextOrderToCook);
						} else {
							console.log("Cafe: There is no work in queue.");
						}
						break;
					case 'orderCookedEvent':
						console.log("Cafe: received orderCookedEvent: " + event.data.payload);
						// remove from cooking
						this.displayCookingItem(null);
						break;
				}
			};

			$("button").click(function() {
				console.log("click");
				var orderName = $(this).attr("data-order");
				$("#order").text(orderName);
				var order: Order = new Order(app.createUuid(), orderName);
				app.waiter.postMessage({'messageName': 'deliverOrderCommand', 'payload' : JSON.stringify(order)});
			});
		}

		private displayQueue(queue: collections.Queue<Order>): void {
			$("#queue").empty();
			queue.forEach((order) => {
				$("#queue").append("<li>" + order.name + "</li>");
			});
		}

		private displayCookingItem(order:Order): void {
			var name = "";
			if (order != null && order.name) {
				name = order.name;
			}
			$("#cooking").text(name);
		}

		private createUuid(): string {
			var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
				var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r&0x3|0x8);
				return v.toString(16);
			});
			return uuid;
		}
	}
}

$(document).ready(function() {
	var cafe = new CafeApp.Cafe();
});