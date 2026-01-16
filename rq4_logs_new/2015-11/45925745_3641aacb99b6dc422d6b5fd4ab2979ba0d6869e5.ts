/**
 * Created by Samuel Gratzl on 15.12.2014.
 */
import C = require('../caleydo_core/main');
import datatypes = require('../caleydo_core/datatype');
import prov = require('../caleydo_provenance/main');
import views = require('../caleydo_core/layout_view');
import ranges = require('../caleydo_core/range');
import databrowser = require('../caleydo_window/databrowser');
import d3 = require('d3');

function setAttributeImpl(inputs, parameter, graph) {
  var gapminder:GapMinder = inputs[0].value,
    name = parameter.name;

  return inputs[1].v.then((data) => {
    const old = gapminder.setAttributeImpl(name, data);
    return {
      inverse: setAttribute(name, inputs[0], old)
    };
  });
}

export function createCmd(id) {
  switch (id) {
    case 'setGapMinderAttribute' :
      return setAttributeImpl;
  }
  return null;
}

export function setAttribute(name: string, $main_ref:prov.IObjectRef<GapMinder>, data:prov.IObjectRef<datatypes.IDataType>) {
  return prov.action(prov.meta('Set Attribute ' + name+' to '+(data?data.name:'<none>'), prov.cat.visual, prov.op.update), 'setGapMinderAttribute', setAttributeImpl, [$main_ref, data], {
    name: name
  });
}

class GapMinder extends views.AView {

  private dim:[number, number];
  ref:prov.IObjectRef<GapMinder>;

  private attrs: {
    x : datatypes.IDataType,
    y : datatypes.IDataType,
    size : datatypes.IDataType,
  } = {
    x: null,
    y: null,
    size: null
  };

  private $elem: d3.Selection<GapMinder>;

  constructor(private elem:Element, private provGraph:prov.ProvenanceGraph) {
    super();
    this.$elem = d3.select(elem).datum(this);
    this.ref = provGraph.findOrAddObject(this, 'GapMinder', 'visual');

    this.init(this.$elem);
  }

  private dropAttribute(sel: d3.Selection<any>, attr: string) {
    databrowser.makeDropable(<Element>sel.node())
      .on('enter', () => sel.classed('over', true))
      .on('enter', () => sel.classed('over', false))
      .on('drop', (event, d) => {
        sel.classed('over', false);
        this.setAttribute(attr, d);
      });
  }

  private init($elem: d3.Selection<any>) {
    const xaxis = $elem.select('div.attr-x');
    this.dropAttribute(xaxis, 'x');
    const yaxis = $elem.select('div.attr-y');
    this.dropAttribute(yaxis, 'y');
    const size = $elem.select('div.attr-size');
    this.dropAttribute(size, 'size');

    //TODO
  }


  private update() {
    //TODO
  }

  reset() {
    this.attrs.x = null;
    this.attrs.y = null;
    this.attrs.size = null;
    this.$elem.selectAll('div.attr-x,div.attr-y,div.attr-size').text('');
    this.update();
  }

  setBounds(x, y, w, h) {
    super.setBounds(x, y, w, h);
    this.dim = [w, h];
    this.relayout();
    this.update();
  }

  relayout() {

  }

  setXAttribute(m:datatypes.IDataType) {
    this.setAttribute('x', m);
  }
  setYAttribute(m:datatypes.IDataType) {
    this.setAttribute('y', m);
  }
  setSizeAttribute(m:datatypes.IDataType) {
    this.setAttribute('size', m);
  }

  setAttributeImpl(attr: string, m: datatypes.IDataType) {
    this.attrs[attr] = m;
    this.$elem.select('div.attr-'+attr).text(m ? m.desc.name : '');

    this.update();
  }

  setAttribute(attr: string, m: datatypes.IDataType) {
    var that = this;
    var mref = this.provGraph.findOrAddObject(m, m.desc.name, 'data');
    that.provGraph.push(setAttribute(attr, this.ref, mref));
  }
}
export function create(parent:Element, provGraph:prov.ProvenanceGraph) {
  return new GapMinder(parent, provGraph);
}