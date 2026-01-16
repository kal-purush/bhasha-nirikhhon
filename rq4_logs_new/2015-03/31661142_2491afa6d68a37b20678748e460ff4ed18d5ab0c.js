function Tree(options) {
	var self = this;

	var template = _.template(options.template);
	var treeData = options.treeData;

	var elem;

	function render() {
		elem = $(template({
			id: "0",
			treeData: treeData,
		})).addClass("tree")
		.on("click", ".toggle-button", onTreeItemClick);
	}

	function toggleTreeItem(treeItem) {
		if (!treeItem.children("ul").length) {
			var id = treeItem.children("input").attr("id");
			var ul = $(template({
				id: id,
				treeData: treeData,
			}));
			treeItem.append(ul);
		};

		treeItem.toggleClass("opened");
	}

	function onTreeItemClick() {
		var treeItem = $(this).parent();

		toggleTreeItem(treeItem);
	}


	self.getElement = function() {
		if (!elem) render();

		return elem;
	}
}