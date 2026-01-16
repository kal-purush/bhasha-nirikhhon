/// <reference path="../../web/app/themes/portfolio/vendors/DefinitelyTyped/threejs/three.d.ts" />
/// <reference path="../../web/app/themes/portfolio/vendors/DefinitelyTyped/greensock/greensock.d.ts" />
/// <reference path="../../web/app/themes/portfolio/vendors/DefinitelyTyped/svgjs/svgjs.d.ts" />
/// <reference path="../core/GLAnimation.ts" />
/// <reference path="../core/Scene3D.ts" />
/// <reference path="../core/EffectComposer.ts" />
/// <reference path="../helper/ThreeAddOns.ts" />
/// <reference path="../helper/ThreeToDom.ts" />
declare var page;

module webglExp {
	interface Point {
	    x:number;
	    y:number;
	}

	export class FloorParticle extends THREE.Vector3 {

		public time:number;

		private startTime:number;
		private currTime:number;
		private duration:number;

		private delay:number;

		constructor(x:number, y:number, z:number) {
		    super(x, y, z);
		    this.reset();
		}

		render() {
			var t:number = Date.now();
			if(t <= this.startTime + this.delay) return;
			this.currTime = t - this.startTime;
	        this.time = this.easeInOutSine(this.currTime,0,1,this.duration);
	        if(this.time > 0.99) {
				this.reset();
	        }
		}

		reset() {
			this.time = 0;
			var t:number =  Date.now();
			this.delay = Math.round(Math.random() * 2000);
			this.startTime = t + this.delay;
			this.duration = Math.round(1000 + Math.random() * 10000);

		}

		easeInOutExpo(t:number, b:number, c:number, d:number):number {
			t /= d/2;
			if (t < 1) return c/2 * Math.pow( 2, 10 * (t - 1) ) + b;
			t--;
			return c/2 * ( -Math.pow( 2, -10 * t) + 2 ) + b;
		}

		easeInOutSine(t:number, b:number, c:number, d:number):number {
			return -c/2 * (Math.cos(Math.PI*t/d) - 1) + b;
		}
	}

	export class Floor extends THREE.PointCloud {
		constructor(geometry:THREE.Geometry, material:THREE.ShaderMaterial) {
		    super(geometry, material);
		}
	}


	export class MaskImg {

		public svg;
		public requestRender:boolean;
		public isDone:boolean;

		private image;
		private imageWidth;
		private imageHeight;
		private imgReady:boolean;
		private rectCtn;
		private rectList:any[];
		private scaleList:any[];
		private untouchedList:any[];
		private gapX:number;
		private gapY:number;

		private renderedNb:number;

		private callback:Function;
		private ctn:HTMLElement;
		private reveal:number;

		private resizeTimeout;

		constructor(url:string, ctn:Node, callback:Function, reveal:number = 0) {
			this.reveal = reveal;
			this.ctn = <HTMLElement>ctn;
			this.callback = callback;
			this.svg = SVG(<HTMLElement>ctn).size(this.ctn.offsetWidth, 0);
			this.image = (<any>this.svg.image(url));
			this.resizeTimeout = -1;
			this.image.loaded(this.imgLoaded);
		}

		imgLoaded = (loader) => {
			this.imageWidth = loader.width;
			this.imageHeight = loader.height;
			var h = this.ctn.offsetWidth / loader.width * loader.height;
			var svgEl:HTMLElement = <HTMLElement>this.svg.node;
			svgEl.style.width = this.ctn.offsetWidth + "px";
			svgEl.style.height = h + "px";
			//this.svg.size(this.ctn.offsetWidth, h);
			this.svg.viewbox(0, 0, this.ctn.offsetWidth, h);
			this.image.size(this.ctn.offsetWidth, h);

			var nb:number = 5;

			var divideX:number = Math.round(this.ctn.offsetWidth / nb);
			var divideY:number = Math.round(h / nb);
			this.gapX = divideX;
			this.gapY = divideY;

			this.scaleList = [];

			this.rectCtn = this.svg.group();

			var sx:number = -divideX * .5;

			for (var i = 0; i < nb; ++i) {
				for (var j = 0; j < nb + 1; ++j) {
					var triangle1 = this.rectCtn.polygon(	(divideX * .5) + "," + (0) + 
															" " + (0) + "," + (divideY) +
															" " + (divideX) + "," + (divideY))
															.fill('#FFFFFF')
															.stroke({width: 0})
															.transform({ scaleX: 0, scaleY: 0});
					this.scaleList.push({	sX: 0, sY: 0, 
											x: sx + divideX * j, y: divideY * i, 
											t: triangle1 });

					var triangle2 = this.rectCtn.polygon(	(divideX * .5) + "," + (divideY) + 
															" " + (divideX) + "," + (0) +
															" " + (0) + "," + (0))
															.fill('#FFFFFF')
															.stroke({width: 0})
															.transform({ scaleX: 0, scaleY: 0});
					this.scaleList.push({	sX: 0, sY: 0, 
											x: divideX * j, y: divideY * i, 
											t:triangle2 });
					
				}
			}

			this.untouchedList = this.scaleList.slice();

			this.sortList(h);

			this.image.maskWith(this.rectCtn);

			this.callback();
			this.imgReady = true;
		}

		sortList(h:number) {
			var cX:number = 0;
			var cY:number = 0;

			switch (this.reveal) {
				case 0:
					cX = 0;
					cY = 0;
					break;
				case 1:
					cX = this.ctn.offsetWidth;
					cY = 0;
					break;
				case 2:
					cX = this.ctn.offsetWidth;
					cY = h;
					break;
				case 3:
					cX = 0;
					cY = h;
					break;
				case 4:
					cX = this.ctn.offsetWidth * .5;
					cY = h * .5;
					break;
			}

			this.scaleList.sort((a,b) => {
				var dx1:number = a.x + this.gapX * .5 - cX;
				var dy1:number = a.y + this.gapY * .5 - cY;
				var dist1:number = dx1 * dx1 + dy1 * dy1;

				var dx2:number = b.x + this.gapX * .5 - cX;
				var dy2:number = b.y + this.gapY * .5 - cY;
				var dist2:number = dx2 * dx2 + dy2 * dy2;

				if(dist1 > dist2) return 1;
				else if(dist1 < dist2) return -1;
				return 0;
			})
		}

		resize() {
			if(!this.imgReady) return;
			window.clearTimeout(this.resizeTimeout);
			this.resizeTimeout = window.setTimeout(this.middleTimeout, 100);
		}

		middleTimeout = () => {
			this.resizeAction();
		}

		resizeAction() {
			if(!this.imgReady) return;
			var w = this.ctn.offsetWidth;
			var h = w / this.imageWidth * this.imageHeight;
			var svgEl:HTMLElement = <HTMLElement>this.svg.node;
			svgEl.style.width = w + "px";
			svgEl.style.height = h + "px";
			this.svg.viewbox(0, 0, w, h);
			this.image.size(w, h);

			var nb:number = 5;

			var divideX:number = Math.round(this.ctn.offsetWidth / nb);
			var divideY:number = Math.round(h / nb);
			this.gapX = divideX;
			this.gapY = divideY;

			this.scaleList = [];

			this.rectCtn = this.svg.group();

			var sx:number = -divideX * .5;

			for (var i = 0; i < nb; ++i) {
				for (var j = 0; j < nb + 1; ++j) {
					var curr:number = ((nb + 1) * i + j) * 2;
					var s1 = this.untouchedList[curr];
					var s2 = this.untouchedList[curr + 1];
					s1.t.plot(	[[divideX * .5, 0], 
								[0, divideY],
								[divideX,divideY]]);

					s1.x = sx + divideX * j;
					s1.y = divideY * i;

					s2.t.plot(	[[divideX * .5, divideY], 
								[divideX, 0],
								[0, 0]]);

					s2.x = divideX * j;
					s2.y = divideY * i;
				}

			}

			this.scaleList = this.untouchedList.slice();

			this.sortList(h);

			if(!this.requestRender) {
				this.requestRender = true;
				this.render();
				this.requestRender = false;
			}
			
		}

		show() {
			this.requestRender = true;
			this.renderedNb = 0;
			this.isDone = true;
			this.resizeAction();
			for (var i = 0; i < this.scaleList.length; ++i) {
				TweenLite.to(this.scaleList[i], 0.5, { sX: 1.1, sY: 1.1, onComplete: this.animDone, delay: i * 0.03, ease:Strong.easeInOut });
			}
		}

		hide() {
			this.requestRender = true;
			this.renderedNb = 0;
			this.resizeAction();
			for (var i = 0; i < this.scaleList.length; ++i) {
				TweenLite.to(this.scaleList[i], 0.1, { sX: 0, sY: 0, onComplete: this.animDone, delay: i * 0.005, ease:Strong.easeInOut });
			}
		}

		animDone = () => {
			this.renderedNb++;
			if(this.renderedNb >= this.scaleList.length) {
				this.requestRender = false;
				// this.image.unmask();
			}
		}

		render() {
			if(!this.requestRender) return;
			for (var i = 0; i < this.scaleList.length; ++i) {
				var s = this.scaleList[i];
				var invScale:number = 1 - s.sX;
				s.t.
				transform({
					x: s.x + invScale * this.gapX * .5,
					y: s.y + invScale * this.gapY * .5,
					scaleX: s.sX,
					scaleY: s.sY
				});
			}
		}

		reset() {
			this.isDone = false;
			for (var i = 0; i < this.scaleList.length; ++i) {
				var s = this.scaleList[i];
				s.sX = s.sY = 0;
				s.t.transform({
					scaleX: s.sX, 
					scaleY: s.sY
				});
			}

			// this.image.maskWith(this.rectCtn);
		}
	} 

	export class Gallery {

		public nbLoaded:number;
		private svg;
		private ctn:Node;

		private ready:boolean;

		private width:number;
		private height:number;
		private imgList:NodeList;
		private svgList:webglExp.MaskImg[];

		private callback:Function;

		constructor(ctn:Node, callback:Function) {
			this.callback = callback;
			this.ctn = ctn;

			this.imgList = (<HTMLElement>ctn).querySelectorAll("img");
			this.svgList = [];
			var nbImage = this.imgList.length;
			this.nbLoaded = 0;
			for (var i = 0; i < nbImage; ++i) {
				var imgMask:webglExp.MaskImg = new webglExp.MaskImg((<HTMLImageElement>this.imgList[i]).getAttribute("data-src"), ctn, this.imgLoaded, Math.floor(Math.random() * 5));
				this.svgList.push(imgMask);
			}			
		}

		resize() {
			for (var i = 0; i < this.svgList.length; ++i) {
				this.svgList[i].resize();
			}
		}

		imgLoaded = () => {
			this.nbLoaded++;
			if(this.nbLoaded >= this.imgList.length) {
				this.launch();
			}
		}

		launch() {
			this.ready = true;
			this.showImage(0);
			this.callback();
		}

		hide() {
			for (var i = 0; i < this.svgList.length; ++i) {
				this.svgList[i].hide();
			}
		}

		reset() {
			for (var i = 0; i < this.svgList.length; ++i) {
				this.svgList[i].reset();
			}
			this.launch();
		}

		showImage(id:number) {
			if(this.ready && !this.svgList[id].isDone) this.svgList[id].show();
		}

		render() {
			if(!this.ready) return;
			for (var i = 0; i < this.svgList.length; ++i) {
				this.svgList[i].render();
			}
		}

		checkImages() {
			var h:number = window.innerHeight;
			for (var i = 0; i < this.svgList.length; ++i) {
				var node:HTMLElement = <HTMLElement>this.svgList[i].svg.node;
				var br = node.getBoundingClientRect();
				var t:number = br.top;
				if(t > 0 && (t + br.height * .5) - h < 0) this.showImage(i);
			}
		}
			
	}

	export class Project extends THREE.Object3D {

		public projectID:number;
		public link:HTMLElement;
		public projectHTML:HTMLElement;
		public galleryReady:boolean;
		public galleryLoading:boolean;

		private planeList:THREE.Mesh[];
		private sizeList:any[];
		private clickPlane:webglExp.ThreeToDom[];
		private three2Dom:webglExp.ThreeToDom;

		private GUTTER:number = 5;

		private size;

		private ctn:THREE.Object3D;
		private camera:THREE.PerspectiveCamera;

		private totalTexture:number;
		private currTexture:number;

		public uniforms;

		private mesh:THREE.Mesh;

		private gallery:webglExp.Gallery;

		private event:CustomEvent;

		private scrollV:number;


		constructor(id:number, camera:THREE.PerspectiveCamera) {
		    super();
		    this.projectID = id;
		    this.planeList = [];
		    this.sizeList = [];
		    this.clickPlane = [];
		    this.camera = camera;
		    this.totalTexture = 6;
		    this.currTexture = 0;

		    this.createButton();

		    var geometry:THREE.TetrahedronGeometry = new THREE.TetrahedronGeometry(200, 2);
		    geometry.computeFaceNormals();
		    geometry.computeVertexNormals();
		   	this.size = { width: 200, height: 200 };	

		    var shaders = GLAnimation.SHADERLIST.dotImage;

			this.uniforms = {
				camPosition: {
			    	type: 'v3',
			    	value: this.camera.position
			  	}
			};

			var mat:THREE.ShaderMaterial =
		  	new THREE.ShaderMaterial({
			    vertexShader:   shaders.vertex,
			    fragmentShader: shaders.fragment,
			    uniforms: this.uniforms,
			    wireframe: true,
			    side: THREE.DoubleSide
		  	});

		  	this.mesh = new THREE.Mesh(geometry, mat);
			this.add(this.mesh);

			this.projectHTML = <HTMLElement>document.getElementById("projects").querySelectorAll('.project').item(id);

			this.gallery = null;
		}

		resize() {
			if(this.gallery !== null) {
				this.gallery.resize();
			}
 		}

		openProject() { 
			if(!this.galleryLoading) {
				this.projectHTML.classList.add("show");
				var text:HTMLElement = (<HTMLElement>this.projectHTML.querySelectorAll(".text").item(0));
				text.classList.add("show");
			}
			if(!this.galleryReady && !this.galleryLoading) {
				var imageDiv:Node = this.projectHTML.querySelectorAll('.images').item(0);
				this.gallery = new webglExp.Gallery(imageDiv, this.launchGallery);
				this.galleryLoading = true;
			} else if(!this.galleryLoading) {
				this.gallery.reset();
			}
		}

		checkGallery() {
			if(this.gallery !== null) {
				this.gallery.checkImages();
			}
		}

		disableRelative = () => {
		}

		launchGallery = () => {
			this.galleryReady = true;
			this.galleryLoading = false;
		}

		getRandomPointAround(signX:number, signY:number):THREE.Vector3 {
			var v:THREE.Vector3 = new THREE.Vector3();
			v.z = this.position.z;

			v.x = signX * (this.size.width * 2);
			v.y = signY * (this.size.height * 2);

			return v;
		}

		update(speed?:number) {
			this.mesh.rotation.y += speed * ((Math.PI * 2) / 100);
		}

		createButton() {
			this.link = <HTMLElement>document.querySelectorAll("#projects-buttons a").item(this.projectID);
			this.link.addEventListener('click', this.goToProject);
		}

		goToProject = (event) => {
			event.preventDefault();
			page(this.link.getAttribute("href"));
		}

		leaveFront() {
			TweenLite.to(this.mesh.scale, 2, { 
				x: 1, 
				y: 1, 
				z: 1, 
				ease: Expo.easeInOut });

			this.gallery.hide();
			(<HTMLElement>this.projectHTML.querySelectorAll(".text").item(0)).classList.remove("show");

			TweenLite.to(document.getElementById("projects"), 0.7, { scrollTop: 0, onComplete:this.hideProject })

		}

		hideProject = () => {
			this.projectHTML.classList.remove("show");
		}

		cameraInFront() {
			TweenLite.to(this.mesh.scale, 2, { 
				x: 0.0001, 
				y: 0.0001, 
				z: 0.0001, 
				ease: Expo.easeInOut })
		}

		render() {
			this.gallery.render();
		}
	}

	export class ProjectsAnimation extends webglExp.GLAnimation {
		private _renderer:THREE.WebGLRenderer;

		private attributes;
		private uniforms;

		private floor:webglExp.Floor;

		private particleList:webglExp.FloorParticle[];
		private composer:webglExp.EffectComposer;
		private copyPass;
		private renderPass;
		private effectBloom;
		private effectBlurH;
		private effectBlurV;
		private effectFXAA;
		private blurh;
		private bloomStrength;


		private projectsList:webglExp.Project[];
		private project:webglExp.Project;

		private cameraCurve:THREE.SplineCurve3;
		private isCamMoving:boolean;
		private isBluring:boolean;
		private posOnPath:number;

		private currProject:number;
		private projectToGo:number;
		private inProject:boolean;
		private scrollVal:number;
		private currScroll:number;

		private mouseVel:webglExp.MouseSpeed;
		private mSpeed:number;

		private buttonList:NodeList;

		private isStarted:boolean;

		private nextPrev:HTMLElement;

		private onProjectY:number;

		private frame:number;

		constructor(scene:THREE.Scene, camera:THREE.PerspectiveCamera, renderer:THREE.WebGLRenderer, index?:number) {

			super(scene, camera, renderer);

			super.setInternalRender(true);

			super.setID("projects");

			super.getLeaveEvent().detail.name = super.getID();

			this._renderer = super.getRenderer();
			this._renderer.autoClear = false;
			this._renderer.gammaInput = true;
		    this._renderer.gammaOutput = true;

		    this.bloomStrength = 0;

			this.attributes = {
			  corridor: {
			    type: 'f',
			    value: []
			  },
			  time: {
			    type: 'f',
			    value: []
			  },
			  displacement: {
			    type: 'f',
			    value: []
			  },
			  gap: {
			    type: 'f',
			    value: []
			  }
			};

			var squareWidth:number = 2000;

			this.uniforms = {
				square: {
			    	type: 'v4',
			    	value: new THREE.Vector4(-squareWidth, -squareWidth, squareWidth * 2, squareWidth * 2)
			  	},
			  	total: {
			    	type: 'f',
			    	value: 10000
			  	},
			  	camPosition: {
			    	type: 'v3',
			    	value: super.getCamera().position
			  	},
			  	timePassed: {
			    	type: 'f',
			    	value: 0
			  	},
			  	alpha: {
			    	type: 'f',
			    	value: 0
			  	},
			  	amplitude: {
			    	type: 'f',
			    	value: 0
			  	}
			};

			var floorGeom:THREE.Geometry = new THREE.Geometry();

			this.frame = 0;
			this.particleList = [];

			for (var i = 0; i < 10000; i++) {
				var v:webglExp.FloorParticle = new webglExp.FloorParticle(1, 0, i);
				var cor = -1;
				if(i%2 === 0) {
					v.x = 0;
					v.y = 1;
				}


				if(Math.floor(i/2)%2 === 0) {
					cor = 1;
				}

				this.attributes.corridor.value.push(cor);
				this.attributes.time.value.push(0);
				this.attributes.time.value.push(0);
				this.attributes.displacement.value.push(Math.random() * 20);
				this.attributes.gap.value.push(i * 0.01);
				floorGeom.vertices.push(v);
				this.particleList.push(v);
			}
			var shaders = GLAnimation.SHADERLIST.projectsAnimation;

			var shaderMaterial:THREE.ShaderMaterial =
		  	new THREE.ShaderMaterial({
			    vertexShader:   shaders.vertex,
			    fragmentShader: shaders.fragment,
			    uniforms: this.uniforms,
			    attributes: this.attributes,
			    side: THREE.DoubleSide,
			    transparent:true
		  	});

		  	this.floor = new webglExp.Floor(floorGeom, shaderMaterial);

		  	this.floor.position.y = -400;

		  	super.getScene().add(this.floor);
		  	this.currProject = -1;
		  	this.project = null;

			this.buttonList = document.querySelectorAll("#projects-buttons a");
		  	this.nextPrev = <HTMLElement>document.getElementById("next-prev");
			Array.prototype.forEach.call(this.buttonList, function(el:HTMLElement, i:number) {
				window.setTimeout(function() {
					el.classList.add("show");
				}, 750 + i * 200);
			}.bind(this));

		  	this.createProjects();

		  	this.mSpeed = 0;
		  	this.mouseVel = new webglExp.MouseSpeed(0.01);

		  	this.createPostEffects();

			TweenLite.to(this.uniforms.alpha, 2, { value: 1.0, ease: Sine.easeOut });

			this.isStarted = false;

			super.getCamera().position.z = 20000;

			this.onProjectY = 0;

			this.scrollVal = this.currScroll = 0;
			document.getElementById("projects").addEventListener("scroll", this.projectScroll);

			this.launchProject(index);

			(<HTMLElement>document.querySelectorAll("#next-prev a.left").item(0)).addEventListener("click", this.prevProject);
			(<HTMLElement>document.querySelectorAll("#next-prev a.right").item(0)).addEventListener("click", this.nextProject);

			(<HTMLElement>document.querySelectorAll("#open-menu").item(0)).addEventListener('click', this.toggleMenu);
		}

		toggleMenu = (event:MouseEvent) => {
			event.preventDefault();
			(<HTMLElement>document.querySelectorAll("#projects-buttons").item(0)).classList.toggle("open");
			(<HTMLElement>document.querySelectorAll("#open-menu").item(0)).classList.toggle("open");
		}

		prevProject = (event:MouseEvent) => {
			event.preventDefault();
			var index:number = this.projectToGo <= 0 ? this.buttonList.length - 1 : this.projectToGo - 1;
			this.launchProject(index);
		}

		nextProject = (event:MouseEvent) => {
			event.preventDefault();
			var index:number = this.projectToGo >= this.buttonList.length - 1 ? 0 : this.projectToGo + 1;
			this.launchProject(index);
		}



		projectScroll = (event:MouseEvent) => {
			var projects:HTMLElement = <HTMLElement>event.target;
			this.projectsMove(projects);
		}

		projectsMove(projects:HTMLElement) {
			var v:number = projects.scrollTop / (projects.scrollHeight - window.innerHeight);
			this.scrollVal = v * -500;

			this.project.checkGallery();
		}

		createPostEffects() {

		    this.composer = new webglExp.EffectComposer(this._renderer, super.getScene(), super.getCamera(), Scene3D.WIDTH, Scene3D.HEIGHT);
			
			this.renderPass = new THREE.RenderPass(this.getScene(), this.getCamera());
			/*var sHorizontal:THREE.Shader = <any>(THREE.HorizontalBlurShader);
			var sVertical:THREE.Shader = <any>(THREE.VerticalBlurShader);*/
			/*this.effectBlurH = new THREE.ShaderPass( sHorizontal );
			this.effectBlurV = new THREE.ShaderPass( sVertical );

			this.blurh = 0;

			this.effectBlurH.uniforms[ 'h' ].value = this.blurh;
			this.effectBlurV.uniforms[ 'v' ].value = this.blurh;*/

			var bloomStrength = 0;
			this.updateBloomBlur(0);
			this.effectBloom = new THREE.BloomPass(bloomStrength, 25, 5.0, 1024);

			

			this.composer.getComposer().setSize(Scene3D.WIDTH * dpr, Scene3D.HEIGHT * dpr);

			var effectFXAAShader:THREE.Shader = <any>(THREE.FXAAShader);
			this.effectFXAA = new THREE.ShaderPass(effectFXAAShader);

			var dpr = 1;
			if (window.devicePixelRatio !== undefined) {
			  dpr = window.devicePixelRatio;
			}
			this.effectFXAA.uniforms['resolution'].value.set(1 / (Scene3D.WIDTH * dpr), 1 / (Scene3D.HEIGHT * dpr));

			this.effectFXAA.renderToScreen = true;

			this.toggleBlurPass(true);

			//this.sceneBlured();

		}

		createProjects() {
			this.projectsList = [];
			var z:number = super.getCamera().position.z - 1000;
			//var camCurves:THREE.Vector3[] = [];
			//camCurves.push(new THREE.Vector3(0, 0, 1000));
			for (var i = 0; i < this.buttonList.length; ++i) {
				var project:webglExp.Project = new webglExp.Project(i, super.getCamera());
		  		super.getScene().add(project);
		  		project.position.set(-100 + Math.random() * 200, -100 + Math.random() * 200, z);
		  		
		  		//camCurves.push(project.getRandomPointAround());

		  		this.projectsList.push(project);


		  		z -= 4000 + Math.random() * 4000;
			}
			
		}

		launchProject(index:number) {
			this.projectToGo = index;
			this.openProject();

			Array.prototype.forEach.call(this.buttonList, function(el:HTMLElement, i:number) {
				el.classList.remove("active");
			}.bind(this));

			(<HTMLElement>this.buttonList.item(index)).classList.add("active");
		}

		openProject() {
			if(this.inProject) this.closeProject();
			else this.showProject();
		}

		closeProject() {
			this.isBluring = true;
			this.projectsList[this.currProject].leaveFront();

			this.nextPrev.classList.remove("show");

			// TweenLite.to(super.getCamera().position, 1, { y : this.onProjectY , expo:Sine.easeOut });
			TweenLite.to(this, 2, { bloomStrength:0, onComplete: this.showProject });
		}

		showProject = () => {
			this.project = null;
			this.project = this.projectsList[this.projectToGo];
			// this.toggleBlurPass(false);
			var project = this.projectsList[this.projectToGo];
			this.isCamMoving = true;
			this.posOnPath = 0;
			this.calcRoute(project);
			var speed:number = Math.abs(this.projectToGo - this.currProject) / this.projectsList.length * (this.projectsList.length * 2);

			this.currProject = this.projectToGo;
			var ease = Expo.easeInOut;
			if(!this.isStarted) {
				speed += 2;
				ease = Strong.easeInOut;
			}

			this.scrollVal = this.currScroll = 0;
			(<HTMLElement>document.getElementById("projects")).scrollTop = 0;

			this.isStarted = true;
			TweenLite.to(this, speed, { posOnPath : 1, ease: ease, onComplete: this.atProject, overwrite:"all" })
		}

		calcRoute(project:webglExp.Project) {
			var camCurves:THREE.Vector3[] = [];
			var camPos:THREE.Vector3 = super.getCamera().position.clone();
			camCurves.push(camPos);
			var min = Math.min(camPos.z, project.position.z);
			var max = Math.max(camPos.z, project.position.z);
			var projList:webglExp.Project[] = this.projectsList.slice();
			var backwards:boolean = false;
			if(camPos.z < project.position.z) {
				projList.sort(function(a, b) {
					return (a.position.z > b.position.z) ? 1 : 0;
				});
				backwards = true;
			}
			var signX:number = 1;
			var signY:number = -1;
			for (var i = 0; i < projList.length; ++i) {
				var p:webglExp.Project = projList[i];
				if(p.position.z < max && p.position.z > min && p.id !== project.id) {
					camCurves.push(p.getRandomPointAround(signX, signY));
					signX *= -1;
					signY *= -1;
				}
			}

			if(backwards) {
				camCurves.push(project.getRandomPointAround(signX, signY));
			}

			var v:THREE.Vector3 = project.position.clone();
			v.z += 500;

			camCurves.push(v);

			this.cameraCurve = new THREE.SplineCurve3(camCurves);

		}

		atProject = () => {
			this.projectsList[this.currProject].cameraInFront();
			this.isCamMoving = false;
			// this.toggleBlurPass(true);

			this.isBluring = true;
			TweenLite.to(this, 2, { bloomStrength:30, onComplete: this.sceneBlured });


			this.inProject = true;
		}

		toggleBlurPass(b:boolean) {
			this.composer.reset();
			this.composer.addPass(this.renderPass);
			this.composer.addPass(this.effectBloom);
			this.composer.addPass(this.effectFXAA);
		}

		sceneBlured = () => {
			this.isBluring = false;

			this.project = this.projectsList[this.currProject];
			this.project.openProject();
			this.nextPrev.classList.add("show");
			this.onProjectY = super.getCamera().position.y;
			this.projectsMove(<HTMLElement>this.project.projectHTML);

			
		}

		render() {
			this.frame += 0.01;
			for (var i = 0; i < this.floor.geometry.vertices.length; i++) {
				var p:webglExp.FloorParticle = this.particleList[i];
				p.render();
				this.attributes.time.value[i] = p.time;
			}

			this.uniforms.amplitude.value = this.frame;

			if(this.isCamMoving) {
				var curvPos:THREE.Vector3 = this.cameraCurve.getPointAt(this.posOnPath);
				var speedZ:number = Math.abs(curvPos.z - super.getCamera().position.z) / 50;
				super.getCamera().position.set(curvPos.x, curvPos.y, curvPos.z);

				this.effectBloom.copyUniforms[ "opacity" ].value = Math.min(7.5, speedZ * 40);
				this.updateBloomBlur(0.3 + speedZ);
				for (var i = 0; i < this.projectsList.length; ++i) {
					this.projectsList[i].update(speedZ);
				}
			} else if(this.isBluring) {
				this.effectBloom.copyUniforms[ "opacity" ].value = this.bloomStrength;
				this.updateBloomBlur(this.bloomStrength / 30 * 2);
			}

			this.floor.position.z = super.getCamera().position.z - 2000.0;

			this.uniforms.timePassed.value = Math.cos(Date.now() * 0.001);
			this.uniforms.camPosition.value = super.getCamera().position;

			this.attributes.time.needsUpdate = true;
			super.render();
 			this.composer.getComposer().render();
 			

 			if(this.project !== null && this.project.galleryReady) {
	 			this.mSpeed += (this.mouseVel.distSquared - this.mSpeed) * 0.05;
 				//this.effectBloom.copyUniforms[ "opacity" ].value = this.bloomStrength;
 				this.updateBloomBlur(1.0 + Math.abs(this.mSpeed * 0.1));

 				this.project.render();
 				this.currScroll += (this.scrollVal - this.currScroll) * 0.1;
 				super.getCamera().position.y = this.currScroll;
 			}

 			
		}

		updateBloomBlur(n:number) {
			this.blurh = n;
			THREE.BloomPass.blurX = new THREE.Vector2( this.blurh / (Scene3D.WIDTH * 2), 0.0 );
			THREE.BloomPass.blurY = new THREE.Vector2( 0.0, this.blurh / (Scene3D.HEIGHT * 2) );
		}

		resize() {
			var dpr = 1;
			if (window.devicePixelRatio !== undefined) {
			  dpr = window.devicePixelRatio;
			}
			this.effectFXAA.uniforms['resolution'].value.set(1 / (Scene3D.WIDTH * dpr), 1 / (Scene3D.HEIGHT * dpr));
			
			this.composer.getComposer().setSize(Scene3D.WIDTH, Scene3D.HEIGHT);

			if(this.project !== null && this.project.galleryReady) {
				this.project.resize();
			}
			super.resize();
		}
	}
}