/** Load the program **/
window.onload = function() {
	var tables = getAllTables();
	makeAllTablesSortable(tables);
}

/** Get the content of the table **/
function getAllTables() {
	tables = document.getElementsByTagName("td");
	return tables;
}

/** Sort tables **/
function makeAllTablesSortable(tables) {
	var head = document.getElementsByTagName("th");

	var flag = new Array();
	for (var k = 0; k < head.length; k++) {
		flag[k] = 0;
	}

	for (var i = 0; i < head.length; i++) {
		head[i].onclick = function() {
			for (var j = 0; j < head.length; j++) {
				/** 
				 *Not the sortby column 
				 *css changes and remove the image on the right side
				**/
				if (this != head[j]) {
					head[j].style.backgroundColor = "rgb(4, 26, 128)";
					var child = head[j].getElementsByTagName("img");
					if (child[0] != undefined)
					    head[j].removeChild(child[0]);
				}

				/** The sortby column **/
				if (this == head[j]) {
					/** Change background color **/
					this.style.backgroundColor = "rgb(168, 178, 240)";

					/** Get the row to be sorted and the sortby array **/
					var dstArray = new Array();
					if (j < 3) {
						var sortArray = new Array(tables[j].innerHTML, tables[j+3].innerHTML, tables[j+6].innerHTML);
						array = document.getElementById("todo").getElementsByTagName("tbody");
					} else if (j >= 3) {
						var index = 9 + j - 3;
						var sortArray = new Array(tables[index].innerHTML, tables[index+3].innerHTML, tables[index+6].innerHTML);
						array = document.getElementById("staff").getElementsByTagName("tbody");
					}
					dstArray = array[0].getElementsByTagName("tr");

					var child = this.getElementsByTagName("img"); // Child is used to judge whether the row is sorted

					if (child[0] == undefined) {
						/** If the row is not sorted **/

						/** Add the image to the right side **/
						var img = document.createElement("img");
						img.src = "ascend.png";

						this.appendChild(img);

						/** Sort **/
						sortArray.sort();
						for (var m = 0; m < sortArray.length; m++) {
							for (var n = 0; n < dstArray.length; n++) {
								if (dstArray[n].innerHTML.match(sortArray[m]) != null) {
									swap(dstArray[m], dstArray[n]);
								}
							}
						}
					} else if (child[0] != undefined) {
						if (flag[j] == 0) {
							/** If the row has been sorted in an ascend order **/
							flag[j] = 1;
							child[0].src = "descend.png";

							/** Sort in an ascend order **/
							swap(dstArray[0], dstArray[2]);

						} else if (flag[j] == 1) {
							/** If the row has been sorted in a descend order **/
							flag[j] = 0;
							child[0].src = "ascend.png";

							/** Sort in a descend order **/
							swap(dstArray[0], dstArray[2]);
						}
					}
				}
			}
		}
	}
}

/** Swap two rows **/
function swap(a, b) {
	var tmp = a.innerHTML;
	a.innerHTML = b.innerHTML;
	b.innerHTML = tmp;
}