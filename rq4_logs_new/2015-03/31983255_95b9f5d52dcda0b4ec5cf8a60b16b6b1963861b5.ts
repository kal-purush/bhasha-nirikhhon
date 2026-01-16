/// <reference path="../../web/app/themes/portfolio/vendors/DefinitelyTyped/threejs/three.d.ts" />
/// <reference path="../../web/app/themes/portfolio/vendors/DefinitelyTyped/greensock/greensock.d.ts" />

module webglExp {
	export class Title {

		private canvas;
		private ctx;

		private mesh:THREE.Mesh;
		private uniforms;
		private attributes;

		private inAnimation:boolean;
		private texture:THREE.Texture;


		constructor() {
			this.canvas = document.createElement("canvas");

			this.ctx = this.canvas.getContext('2d');

			this.canvas.setAttribute("width", "1100px");
			this.canvas.setAttribute("height", "100px");
			/*this.ctx.fillStyle = "white";
			this.ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);*/
			

			this.texture = new THREE.Texture(this.canvas);
			this.texture.needsUpdate = true;
			var geom:THREE.PlaneBufferGeometry = new THREE.PlaneBufferGeometry(this.canvas.width, this.canvas.height);
			
			var shaders = GLAnimation.SHADERLIST.text;

			this.uniforms = {
				tText: 	{ 
					type: "t", 
					value: this.texture 
				},
				time: {
					type: 'f',
					value: 0.0
				},
				cosTime: {
					type: 'f',
					value: 1.0
				},
				noiseVal: {
					type: 'f',
					value: 0.0
				},
				cutRatio: {
					type: 'f',
					value: 3.0
				}
			};

			this.attributes = {
			 
			};

			this.inAnimation = false;

			var mat:THREE.ShaderMaterial =
		  	new THREE.ShaderMaterial({
			    vertexShader:   shaders.vertex,
			    fragmentShader: shaders.fragment,
			    uniforms: this.uniforms,
			    attributes: this.attributes,
			    side: THREE.DoubleSide,
			    transparent: true
		  	});

			this.mesh = new THREE.Mesh(geom, mat);
		}

		public setText(str:string) {

			this.ctx.fillStyle = "white";
			this.ctx.font = "60pt 'Lato'";
			var tSize:number = Math.max(this.ctx.measureText(str).width, 100);
			this.ctx.fillText(str, (this.canvas.width - tSize) * 0.5, 80);
			this.texture.needsUpdate = true;
		}

		public getObject():THREE.Mesh {
			return this.mesh;
		}

		public show() {
			this.uniforms.cutRatio.value = 2 + Math.floor(Math.random() * 7);
			this.inAnimation = true;
			TweenLite.to(this.uniforms.cosTime, 1.5, { value: 0, ease:Expo.easeInOut, onComplete: this.endAnim });
		}

		public hide() {
			this.uniforms.cutRatio.value = 2 + Math.floor(Math.random() * 7);
			TweenLite.to(this.uniforms.cosTime, 1.5, { value: 1, ease:Expo.easeInOut });
		}

		public render() {
			this.uniforms.time.value += 0.01;
			if(this.inAnimation) {
				this.uniforms.noiseVal.value = 1 + Math.random() * 3;
			}
		}

		private endAnim = () => {

			this.inAnimation = false;
		}
	}
}