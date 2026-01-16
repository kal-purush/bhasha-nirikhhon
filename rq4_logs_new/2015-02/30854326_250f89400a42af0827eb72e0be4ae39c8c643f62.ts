/// <references path="table.ts"/>

class TableEvents {
	public mouseUpEvent : (e : MouseEvent) => void;

	public registerEditingEvents(div : HTMLDivElement) {
		
	}

	public constructor(public table : Table) {
		// mousedown in table
        this.table.tableElement.addEventListener("mousedown", e => {
            switch (this.table.state) {
                case 0:
                    this.table.a = Point.fromTarget(e.target);
                    this.table.state = 1;
                    break;
                case 2:
                    if (e.target === this.table.grid.get(this.table.a)) {
                        this.table.state = 6;
                    } else {
                        this.table.a = Point.fromTarget(e.target);
                        this.table.state = 1;
                    }
                    break;
                case 4:
                        this.table.b = null;
                        this.table.a = Point.fromTarget(e.target);

                        this.table.state = 1;
                    break;
                case 5:
                    if (e.target !== this.table.grid.get(this.table.a)) {
                        this.table.a = Point.fromTarget(e.target);
                        this.table.state = 1;
                    }
                }
                e.stopPropagation();
        });

		this.table.tableElement.addEventListener("mousemove", e => {
            switch (this.table.state) {
            case 6:
                this.table.state = 1;
            }
        });

        // off table listener
        document.addEventListener("mousedown",
            e => {
                    switch (this.table.state) {
                    case 0:
                    case 2:
                    case 4:
                    case 5:
                        this.table.state = 0;
                    }
                });


        this.mouseUpEvent = e => {
            switch (this.table.state) {
            case 1:
                this.table.state = 2;
                break;
            case 3:
                this.table.state = 4;
                break;
            case 6:
                this.table.state = 5;
                break;
                
            }
            e.stopPropagation();
        };

        this.table.tableElement.addEventListener("mouseup", this.mouseUpEvent);
        
        this.table.tableElement.addEventListener("mouseout", e => {
            switch (this.table.state) {
            case 1:         
                var t = Point.fromTarget(e.target);
                
                console.log("mouseOut: target(" + t.x + ", " + t.y + ") a(" + this.table.a.x + ", " + this.table.a.y + ")" );
                console.log(Point.fromTarget(e.target).equals(this.table.a));
//                if (e.target === this.table.grid.get(this.table.a)) {
                if (Point.fromTarget(e.target).equals(this.table.a)) {
                    this.table.b = Point.fromTarget(e.relatedTarget);
                    this.table.state = 3;
                }
            }
            e.stopPropagation();
        });
        
        this.table.tableElement.addEventListener("mouseover", e => {
            switch (this.table.state) {
            case 3:
                if (e.target == this.table.grid.get(this.table.a)) {
                    this.table.b = null;
                    this.table.state = 1;
                } else {
                    if (e.target === null) {
                        // might have moved off the table
                    }
                    this.table.b = Point.fromTarget(e.target);
                }
            }
            e.stopPropagation();
        });

        document.addEventListener("keypress", e => {
            switch (this.table.state) {
            case 2:
//                var charCode = (typeof e.which == "number") ? e.which : e.keyCode;
//                this.table.grid.get(this.table.a).textContent = String.fromCharCode(charCode);
                this.table.state = 5;
            }
        });
        
        Mousetrap.bind("esc", e => {
            switch (this.table.state) {
            case 5:
                console.log("escape!");
                this.table.state = 2;
                return false;
            }
        });
        
        Mousetrap.bind("tab", e => {
            switch (this.table.state) {
            case 5:
                this.table.state = 2;
                this.table.a = new Point(this.table.a.x+1, this.table.a.y);
                return false;
            }
        });
        
        Mousetrap.bind("enter", e => {
            switch (this.table.state) {
            case 5:
                this.table.state = 2;
                this.table.a = new Point(this.table.a.x, this.table.a.y+1);               
                return false;
            }
        });
        
        var __this = this;

        Mousetrap.bind("left", e => {
            
            
            switch (this.table.state) {
            case 4:
                this.table.b = null;
                this.table.state = 2; // and drop down into case 2
            case 2:
                this.table.a = new Point(this.table.a.x-1, this.table.a.y);
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind("up", e => {
            

            switch (this.table.state) {
            case 4:
                this.table.b = null;
                this.table.state = 2; // and drop down into case 2
            case 2:
                this.table.a = new Point(this.table.a.x, this.table.a.y-1);
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind("down", e => {
            
            switch (this.table.state) {
            case 4:
                this.table.b = null;
                this.table.state = 2; // and drop down into case 2
            case 2:
                this.table.a = new Point(this.table.a.x, this.table.a.y+1);
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind("right", e => {
            
            switch (this.table.state) {
            case 4:
                this.table.b = null;
                this.table.state = 2; // and drop down into case 2
            case 2:
                this.table.a = new Point(this.table.a.x+1, this.table.a.y);
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind("shift+left", e => {
            
            switch (this.table.state) {
            case 2:
                this.table.b = new Point(this.table.a.x-1, this.table.a.y);
                this.table.state = 4;
                return false;
            case 4:
                this.table.b = new Point(this.table.b.x-1, this.table.b.y);
                if (this.table.b.equals(this.table.a)) {
                    this.table.b = null;
                    this.table.state = 2;
                }
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind("shift+up", e => {
            
            switch (this.table.state) {
            case 2:
                this.table.b = new Point(this.table.a.x, this.table.a.y-1);
                this.table.state = 4;
                return false;
            case 4:
                this.table.b = new Point(this.table.b.x, this.table.b.y-1);
                if (this.table.b.equals(this.table.a)) {
                    this.table.b = null;
                    this.table.state = 2;
                }
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind("shift+down", e => {
            
            switch (this.table.state) {
            case 2:
                this.table.b = new Point(this.table.a.x, this.table.a.y+1);
                this.table.state = 4;
                return false;
            case 4:
                this.table.b = new Point(this.table.b.x, this.table.b.y+1);
                if (this.table.b.equals(this.table.a)) {
                    this.table.b = null;
                    this.table.state = 2;
                }
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind("shift+right", e => {
            
            switch (this.table.state) {
            case 2:
                this.table.b = new Point(this.table.a.x+1, this.table.a.y);
                this.table.state = 4;
                return false;
            case 4:
                this.table.b = new Point(this.table.b.x+1, this.table.b.y);
                if (this.table.b.equals(this.table.a)) {
                    this.table.b = null;
                    this.table.state = 2;
                }
                return false;
            }
            
            return true;
        });
        
        Mousetrap.bind(["delete", "backspace"], e => {
            switch (this.table.state) {
            case 2:
                this.table.grid.get(this.table.a).textContent = "";
                return false;
            case 4:
                var cells = this.table.tableElement.querySelectorAll(".selected");
                for (var i = 0; i < cells.length; ++i) {
                    cells.item(i).textContent = "";
                }
                return false;
            }
        });
	}
}