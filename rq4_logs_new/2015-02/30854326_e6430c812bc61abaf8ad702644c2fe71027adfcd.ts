
class TableOperations {
	public constructor(public table : Table) {
		document.querySelector("#merge_cells").addEventListener("click",
			this.merge_cells, true);

		// prevent buttons from triggering deselection of table
		document.querySelector(".toolbar").addEventListener("mousedown",
			e => {
				e.stopPropagation();
				}, true);
		document.querySelector(".toolbar").addEventListener("mouseup",
			e => {
				e.stopPropagation();
				}, true);
		
	}

	private add_row_above = (e : MouseEvent) => {
		console.log("add row above");
		return false;
	}

	private add_row_below = (e : MouseEvent) => {
			
		return false;
	}
	private add_column_before = (e : MouseEvent) => {
		
		return false;
	}
	private add_column_after = (e : MouseEvent) => {
		
		return false;
	}
	private delete_row = (e : MouseEvent) => {
		
		return false;
	}
	private delete_column = (e : MouseEvent) => {
		
		return false;
	}

	private merge_cells = (e : MouseEvent) => {
		if (this.table.state === 4) {
			this.table.mergeCells();
		} else if (this.table.state === 2) {
			this.table.splitCell();	
		} else {
			throw "Invalid state detected for merging: " + this.table.state;
		}
		
		return false;
	}
}