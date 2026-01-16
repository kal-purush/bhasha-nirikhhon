///<reference path="Point.ts"/>


module com.hydrotik.geom {

	import Point = com.hydrotik.geom.Point;


	export class Rectangle {

		private _br:Point;
		private _tl:Point;

		public height:number;
		public width:number;
		public x:number;
		public y:number;

		public get bottom():number{
			return this.y + this.height;
		}

		public get bottomRight():Point{
			if (this._br == null){
				this._br = new Point();
			}

			this._br.x = this.x + this.width;
			this._br.y = this.y + this.height;

			return this._br;
		}

		public get left():number{
			return this.x;
		}

		public get right():number{
			return this.x + this.width;
		}

		public get top():number{
			return this.y;
		}

		public get topLeft():Point{
			if (this._tl == null){
				this._tl = new Point();
			}

			this._tl.x = this.x;
			this._tl.y = this.y;

			return this._tl;
		}


		constructor(x:number = 0, y:number = 0, width:number = 0, height:number = 0){
			this.x = x;
			this.y = y;
			this.width = width;
			this.height = height;
		}

		public clone():Rectangle{
			return new Rectangle(this.x, this.y, this.width, this.height);
		}

		public toString():string{
			return 'Rectangle x: ' + this.x + ', y: ' + this.y + ', width: ' + this.width + ', height: ' + this.height;
		}
	}
}