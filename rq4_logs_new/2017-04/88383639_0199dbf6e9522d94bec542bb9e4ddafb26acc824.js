function Enneaphalanx(sceneGraph, startId, constructState){
    this.config = {
	start_id: startId
    };
    this["scene graph"] = sceneGraph;
    this.start = Object.create(this.start);
    this.start.parent = this;
    this.event = Object.create(this.event);
    this.event.click = function canvas_click(){
	return this.event.do_click(this);
    };
    this.event.parent = this;
    this.dom = Object.create(this.dom);
    this.dom.parent = this;
    this.scene = Object.create(this.scene);
    this.scene.parent = this;
    this.state = constructState(this);
}