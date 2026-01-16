class ToggleSidebar {
	name = "";
	evtOpts = false;
	addedSidebar = false;
	initialized = false;

	constructor(element) {
		//Generating bind actions to listener functionality
		this.openActionHandler = this.openSidebar.bind(this);
		this.closeActionHandler = this.closeSidebar.bind(this);
		this.bodyActionHandler = this.sidebarBodyListener.bind(this);

		//verifying constructor element or initializing sidebars functionality
		if (typeof element !== "undefined") {
			this.addSidebar(element);
		} else {
			this.init();
		}
	}

	addSidebar(sidebar) {
		//function to add a sidebar. This adds to HTML nodes sidebar data- attributes

		let errorText = "Configuration error when adding a toggle sidebar:";
		let errorMsg = [];
		/*
		verifying errors
		*/
		if (typeof sidebar !== "object") {
			//if function param is not and object
			errorMsg.push("addSidebar function requires an JS object.");
		}
		if (typeof sidebar.containerNode === "undefined") {
			//if container HTML node was not specified
			errorMsg.push("An sidebar node (container) was not specified.");
		}

		if (typeof sidebar.activationClass === "undefined") {
			//if container activation class name was not specified
			errorMsg.push("An activation CSS class was not specified.");
		}

		if (typeof sidebar.openSidebarNode === "undefined") {
			//if HTML open node was not specified
			errorMsg.push("An opener (activate) node was not specified.");
		}
		if (errorMsg.length > 0) {
			//throw error (finalize executuion) if any error was found
			this.die(errorText, errorMsg);
		}

		//if not sidebar name was specified a sidebar name will be created with current timestamp
		if (typeof sidebar.name === "undefined") {
			sidebar.name = "ToggleSidebar-" + Date.now();
			console.warn(
				'An sidebar name was not specified. "' +
					sidebar.name +
					'" will be used instead '
			);
		}

		//set the autoclose to true (close when click out container) when no close node was
		//specified. If autoClose attribute is in function param object it will be used. Default is false
		let autoclose = !sidebar.closeSidebarNode || sidebar.autoClose || false;

		//verifying container and open nodes exists
		if (
			typeof sidebar.containerNode !== "object" ||
			sidebar.containerNode === null
		) {
			errorMsg.push(
				"Especified sidebar HTML node container is not valid or not found"
			);
		} else {
			//setting the HTML container node data- attributes in function parameter object
			const contData = sidebar.containerNode.dataset;
			contData.sidebarContainer = "true";
			contData.sidebar = sidebar.name;
			contData.sidebarActivateClass = sidebar.activationClass;
			contData.sidebarAutoclose = autoclose;
		}
		if (
			typeof sidebar.openSidebarNode !== "object" ||
			sidebar.openSidebarNode === null
		) {
			errorMsg.push(
				"Especified open sidebar node is not valid or not found"
			);
		} else {
			//setting the HTML open node data- attributes in function parameter object
			const openElData = sidebar.openSidebarNode.dataset;
			openElData.sidebar = sidebar.name;
			openElData.sidebarAction = "open";
		}

		//if close node was specified in function parameter object
		if (typeof sidebar.closeSidebarNode !== "undefined") {
			if (
				typeof sidebar.closeSidebarNode !== "object" ||
				sidebar.closeSidebarNode === null
			) {
				errorMsg.push(
					"Especified close sidebar node is not valid or not found"
				);
			} else {
				const closeElData = sidebar.closeSidebarNode.dataset;
				closeElData.sidebar = sidebar.name;
				closeElData.sidebarAction = "close";
			}
		}
		if (errorMsg.length > 0) {
			//throw error (finalize executuion) if any error was found
			this.die(errorText, errorMsg);
		} else {
			this.addedSidebar = true;
			this.init();
		}
	}

	die(text, msgArray) {
		//Function to show an error in console and end execution of JS script
		let errorCounter = 0;
		text = "\n" + text + "\n";
		msgArray.forEach((msg) => {
			errorCounter++;
			text += errorCounter + ". " + msg + "\n";
		});
		throw new Error(text);
	}

	init() {
		//Plugin initialization reading sidebar data- attributes

		//Getting HTML body tag
		this.body = document.querySelector("body");
		//sidebar containers
		this.sidebars = document.querySelectorAll("[data-sidebar-container]");
		//sidebar action buttons (open and close)
		this.sidebarsElements = document.querySelectorAll(
			"[data-sidebar-action]"
		);

		if (this.sidebars.length > 0) {
			this.addedSidebar = true;
		}
		this.sidebarsList = [];

		//getting and setting sidebars relative data (name, activation class,
		//autoclose, openStatus) and action nodes
		this.sidebars.forEach((node) => {
			let sidebarInfo = {};
			sidebarInfo.name = node.dataset.sidebar;
			sidebarInfo.activationClass = node.dataset.sidebarActivateClass;
			sidebarInfo.containerNode = node;
			this.sidebarsElements.forEach((el) => {
				if (sidebarInfo.name == el.dataset.sidebar) {
					if (el.dataset.sidebarAction == "open") {
						sidebarInfo.openSidebarNode = el;
					}
					if (el.dataset.sidebarAction == "close") {
						sidebarInfo.closeSidebarNode = el;
					}
				}
			});
			let autoClose = node.dataset.sidebarAutoclose == "true" || false;
			if (!sidebarInfo.closeSidebarNode) {
				autoClose = true;
			}
			sidebarInfo.autoClose = autoClose;
			sidebarInfo.openStatus = false;
			this.sidebarsList.push(sidebarInfo);
		});
		if (this.sidebars.length > 0 && this.addedSidebar) {
			this.execute();
			this.initialized = true;
		}
	}

	closeSidebar(sidebar, force) {
		// function to close sidebar. If force parameter is not true or present a
		// body listener is added to run sidebarBodyListener function
		// (this.bodyActionHandler) on next user click. The only action to
		// close sidebar is remove the sidebar activation CSS class in body HTML tag

		if (force) {
			//removing body listener
			this.body.removeEventListener("click", this.bodyActionHandler, false);
			// removing activation class
			this.body.classList.remove(sidebar.activationClass);
			// remove saidebar open status
			sidebar.openStatus = false;
			// adding event listener to sidebar open node
			sidebar.openSidebarNode.addEventListener(
				"click",
				this.openActionHandler,
				false
			);
		} else {
			// adding listener to body. This will compare the next user clicked node
			this.body.addEventListener("click", this.bodyActionHandler, false);
		}
	}

	execute() {
		// function to create listeners on sidebars open nodes

		// console warning if plugins was not initlialized (init()) or sidebar added
		if (!this.addedSidebar && !this.initialized) {
			console.warn(
				'\nNo sidebar specified with "data-sidebar" attributes in HTML or an JS object.\n'
			);
			return;
		}
		//removing any possible event listener relative to ToggleSidebar in
		//HTML body tag or sidebars open nodes
		this.body.removeEventListener("click", this.bodyActionHandler, false);
		this.sidebarsList.forEach((sidebar) => {
			sidebar.openSidebarNode.removeEventListener(
				"click",
				this.openActionHandler,
				false
			);
			sidebar.openSidebarNode.addEventListener(
				"click",
				this.openActionHandler,
				false
			);
		});
	}

	openSidebar(event) {
		//function to open sidebars
		//The real action to open a sidebar is adding to body the activation CSS class

		//stop event propagation to avoid double click actions
		event.stopPropagation();

		//clicked element
		const clickedElement = event.target;
		this.sidebarsList.forEach((sidebar) => {
			if (clickedElement.dataset.sidebar == sidebar.name) {
				//removing open listener to add later a close function to
				//sidebar open node
				sidebar.openSidebarNode.removeEventListener(
					"click",
					this.openActionHandler,
					false
				);
				// adding sidebar activation class to HTML body node
				this.body.classList.add(sidebar.activationClass);
				sidebar.openStatus = true;

				//Timeout to add the close functionality in open node
				setTimeout(
					(that) => {
						that.closeActionHandler(sidebar, false);
					},
					50,
					this
				);
			} else if (sidebar.openStatus == true) {
				//if any sidebar is opened for any reason and is not the
				//clicked open sidebar node relative sidebar node, foreced sidebar
				//close command is triggered to avoid showing double sidebar
				this.closeActionHandler(sidebar, true);
			}
		});
	}

	sidebarBodyListener(evt) {
		//function to listen any click and compare if click will trigger
		//the close sidebar action

		//stop event propagation to avoid double click actions
		evt.stopPropagation();

		//clicked element
		const clicked = evt.target;
		let sidebar;

		//searching the first sidebar with openStatus = true.
		//Any other sidebar was closed in openSidebar function
		this.sidebarsList.forEach((testbar) => {
			if (testbar.openStatus) {
				sidebar = testbar;
				return;
			}
		});
		//getting close node
		const closeIcon = sidebar.closeSidebarNode;
		//getting open node
		const openIcon = sidebar.openSidebarNode;
		//getting sidebar container node
		const sidebarClicked = clicked.closest(
			"[data-sidebar=" + sidebar.name + "]"
		);
		//comparing clicked element with open, close or inside sidebar
		if (
			clicked == openIcon ||
			clicked == closeIcon ||
			(sidebar.autoClose && sidebarClicked === null)
		) {
			//removing body event listener in body (this helps avoid double listener)
			this.body.removeEventListener("click", this.bodyActionHandler, false);
			//forcing sidebar close
			this.closeActionHandler(sidebar, true);
		}
	}
}

/*
USE EXAMPLE
window.addEventListener("DOMContentLoaded", (event) => {
	const sidebar = new ToggleSidebar();

	sidebar.addSidebar({
		name: "login-sidebar",
		activationClass: "show-login",
		autoClose: true,
		closeSidebarNode: document.querySelector("#login-container .icon-close"),
		containerNode: document.getElementById("login-container"),
		openSidebarNode: document.querySelector("#login-button"),
	});
});
*/