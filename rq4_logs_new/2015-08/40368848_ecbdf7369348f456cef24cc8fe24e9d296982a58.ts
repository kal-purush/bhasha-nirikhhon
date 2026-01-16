import IViewEntityNS = require("./ViewEntity");
import IViewEntity = IViewEntityNS.IViewEntity;
import Core = require("./Gl/core");

class ObjectUniformsProvider extends IViewEntityNS.ViewEntityBase {
    constructor(public uniformObject : any, parent : IViewEntity = null) {
	super(parent);
    }
    getValue(k:string) : Core.GlValue {
	if(this.uniformObject && this.uniformObject[k] !== undefined) {
	    var v = this.uniformObject[k];
	    if(v instanceof Function){
		return v.call(this.uniformObject, this); 
	    }
	    return v;
	}
	return this.parent && this.parent.getValue(k);
    }
    getProvidedValues() {
	return Object.keys(this.uniformObject);
    }
}

class ViewGroup extends ObjectUniformsProvider {
    constructor(public children: IViewEntity[] = [], obj : any = null) {
	super(obj);
	this.children.forEach(c => c.parent = this);
    }
    add( entity : IViewEntity ) {
        this.children.push(entity);
	entity.parent = this; 
    }
    render() {
	super.render(); 
        this.children.forEach(e => e.render());
    }

}

export = ViewGroup;