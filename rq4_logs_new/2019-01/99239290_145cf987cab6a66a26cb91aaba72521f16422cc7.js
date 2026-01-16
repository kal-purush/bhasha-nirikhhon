const data = {
    simpleEx:[
      {
        "Child": "Attacker",
        "Parent": "Get a Shovel",
        "Group": "Aquire Tools"
      },
      {
        "Child": "Get a Shovel",
        "Parent": "Tunnel Under the Moat",
        "Group": "Breach Castle"
      },
      {
        "Child": "Tunnel Under the Moat",
        "Parent": "Surface in the Courtyard",
        "Group": ""
      },
      {
        "Child": "Surface in the Courtyard",
        "Parent": "Descend to the Treasure Room",
        "Group": ""
      },
      {
        "Child": "Tunnel Under the Moat",
        "Parent": "Tunnel to the Treasure Room",
        "Group": ""
      },
      {
        "Child": "Descend to the Treasure Room",
        "Parent": "Steal Guard's Keys",
        "Group": "Bypass Door"
      },
      {
        "Child": "Steal Guard's Keys",
        "Parent": "Enter the Treasure Room",
        "Group": ""
      },
      {
        "Child": "Descend to the Treasure Room",
        "Parent": "Pick the Lock",
        "Group": "Bypass Door"
      },
      {
        "Child": "Attacker",
        "Parent": "Get a Rope",
        "Group": "Aquire Tools"
      },
      {
        "Child": "Get a Rope",
        "Parent": "Scale the Castle Wall",
        "Group": "Breach Castle"
      },
      {
        "Child": "Scale the Castle Wall",
        "Parent": "Descend to the Treasure Room",
        "Group": ""
      },
      {
        "Child": "Tunnel to the Treasure Room",
        "Parent": "Steal the Jewels",
        "Group": ""
      },
      {
        "Child": "Pick the Lock",
        "Parent": "Enter the Treasure Room",
        "Group": ""
      },
      {
        "Child": "Enter the Treasure Room",
        "Parent": "Steal the Jewels",
        "Group": ""
      }],
    twoEndStates:[]
  }
  
  const style = [
    {
        "selector": "node",
        "style": {
        'content':'data(label)',
        'shape': 'rectangle',
        'background-fit': 'cover',
        'border-color': 'black',
        'background-color':'white',
        'border-width': 3,
        'border-opacity': 0.5,
        'width':110,
        'height': 'mapData(height, 0, 120, 0, 120)',
        'text-valign': "center",
        'text-halign': "center"
        }
    },
    {
        "selector":"node.state-node",
        "style":{
        'background-color': 'data(nodeColor)',
        'color': 'data(fontColor)',
        'text-wrap': "wrap",
        'text-max-width': 100
        }
    },
    {
        "selector":"node.starting-node",
        "style":{
        'shape': 'ellipse',
        'text-wrap': "wrap",
        'text-max-width': 100
        }
    },
    {
        "selector":"node.root-node",
        "style":{
        'width':150,
        'height':150,
        'font-size':'1.2em',
        'font-weight':'bold',
        'shape': 'octagon',
        'text-wrap': "wrap",
        'text-max-width': 100,
        //'background-color': '#dadada'
        }
    },
    {
        selector: 'node.wrapper-node',
        css: {
        'padding-top': '10px',
        'padding-left': '10px',
        'padding-bottom': '10px',
        'padding-right': '10px',
        'text-valign': 'top',
        'text-halign': 'center',
        'font-weight':'bold',
        'font-size': "1.5em",
        'background-color': '#f6f6f6',
        'text-max-width': 999999
        }
    },
    {
        selector: 'node.cy-expand-collapse-collapsed-node',
        css:{
        'text-valign': 'center',
        'text-halign': 'center',
        'width': 110,
        'height': 100,
        'text-max-width':100,
        'text-wrap': "wrap",
        'font-size':"1em",
        'background-color': 'data(nodeColor)',
        'color': 'data(fontColor)'
        }
    },
    {
        "selector": "edge",
        "style": {
        'curve-style': 'bezier',
        'target-arrow-shape': 'triangle',
        'line-color': '#828282',
        'target-arrow-color': '#828282',
        }
        },
    ]
  
  let rankdir = "BT";
  let dataSelect = 'simpleEx';
  let shouldGroupNodes = 'true';
  let enableDrag = 'false';
  let expandingNode = false;
  let collapsingNode = false;
  
  function formatNodes(data){
    
    function findNode(val,accessor){
      return data.find((d)=>{
        return d[accessor] === val
       })
    }
    
    let returnArr = [];
  
    [...new Set(data.map(d=>d.Parent))].forEach((d)=>{
      const foundNode = findNode(d,"Parent") 
      returnArr.push({
        data:{
          id:d,
          label:d,
          parent:foundNode.Group ? foundNode.Group.length > 0 ? foundNode.Group : undefined : undefined,
          height:d.length > 45 ? 120 : 75,
          nodeType: data.map(x=>x.Child).indexOf(d) === -1 ? 'Root Node' : 'State Node',
          nodeColor: '#D3D3D3',
          fontColor: 'black'
        }, 
        classes: data.map(x=>x.Child).indexOf(d) === -1 ? 'root-node' : 'state-node'
      })
  
    });
  
      //Get the parent (wrapper) nodes if they exist
      if(data[0].Group && shouldGroupNodes === "true"){
        [...new Set(data.map(d=>d.Group).filter(d=>d.length>0))].forEach((d)=>{
            returnArr.push({
              data:{
                id:d,
                label:`${d}\n\n(x${returnArr.map(d=>d.data.parent).filter(x=>x===d).length})`,
                nodeType: 'Grouped Node',
                nodeColor:'#D3D3D3',
                fontColor:'black'
              },
              classes:'wrapper-node'
            })
        })
      };
  
      //Getting starting nodes
      [...new Set(data.map(d=>d.Child).filter((d)=>{ return returnArr.map(d=>d.data.id).indexOf(d) === -1 }))]
      .forEach((d)=>{
        const foundNode = findNode(d,"Child")
        returnArr.push({
          data:{
            id:d,
            label:d,
            nodeType:'Start Node',
            height:d.length > 45 ? 120 : 75
          },
          classes:'starting-node'
        })
      });
    
      return returnArr
  }
  
  function formatEdges(data){
    return data.map((d)=>{
      return {
          data:{
            source:d.Child,
            target:d.Parent
          }
        }
      })
  }
  
  function initEventListeners(){
    //Node list
    Array.from(document.getElementsByTagName('input')).forEach((d)=>{
      d.addEventListener('change', function (evt) {
        rankdir = this.value;
        initCytoscape();
      });
    })
    
    document.getElementById('dataSelect').addEventListener('change', function(evt){
      dataSelect = this.value;
      initCytoscape();
    })
    
    document.getElementById('groupSelect').addEventListener('change', function(evt){
      shouldGroupNodes = this.value;
      initCytoscape();
    })
    
     document.getElementById('dragSelect').addEventListener('change', function(evt){
      enableDrag = this.value;
      initCytoscape();
    })
  }
  
  function handleNodeClicks(cy){
    cy.on('tapstart',function(event){
      //Edge taps dont do anything
      if(event.target._private.group === 'edges'){ return }
      //Background tap
      else if(event.target === cy){
        cy.viewUtilities().removeHighlights()
      }
      //Prevent tap events if a node is expanding or collapsing (using state)
      else if(collapsingNode){
        cy.viewUtilities().removeHighlights()
        collapsingNode = !collapsingNode
        return
      }
      else if(expandingNode){
        expandingNode = !expandingNode
        return
      }
      //Else, register tap events on nodes
      else{
        cy.viewUtilities().removeHighlights()
        cy.viewUtilities().highlightNeighbors(event.target)
       }
    })
  
    //Set up propagation blockers on espand/collapse, they get registered as node events, so we have to handle them seperately as a state object (annoying)
    cy.nodes().on("expandcollapse.beforecollapse", function(e) {
      collapsingNode = true
    })
    cy.nodes().on("expandcollapse.beforeexpand", function(e) { 
      expandingNode = true
    })
  
    cy.nodes().on("expandcollapse.afterexpand", function(e) { 
      //Prevent drag on freshly expanded nodes
      if(enableDrag === 'false'){
        cy.nodes().ungrabify()
      }
       cy.viewUtilities().removeHighlights()
    })
  
    cy.nodes().on("expandcollapse.aftercollapse", function(e) { 
      cy.viewUtilities().removeHighlights()
    })
  }
  
  function initCytoscape(){
    
    const layout = {
          name: 'dagre',
          rankDir: rankdir,
          padding:20,
          directed: true,
          fit:true, 
          randomize: false,
          nodeDimensionsIncludeLabels: true,
          animate: 'end',
          animationEasing: 'ease-out',
          animationDuration: 1000,
     }
  
     const cy = cytoscape({
      container: document.getElementById('cytoscape'),
        boxSelectionEnabled: enableDrag === 'false' ? false : true,
        autounselectify: enableDrag === 'false' ? true : false,
        style: style,
        elements: {
          nodes: formatNodes(data[dataSelect]),
          edges: formatEdges(data[dataSelect])
         },
       }); // cy init
  
        // Add ability to expand and collapse nodes
        cy.expandCollapse({
          layoutBy: layout,
           fisheye: false,
           animate: true
        });
  
        const ur = cy.undoRedo()
  
        cy.ready(function(){
          ur.do("collapseAll")
          cy.layout(layout).run()
        })
  
        //Disable node 
        if(enableDrag === 'false'){
          cy.nodes().ungrabify()
        }
     
        //Pin max/min zoom
        cy.maxZoom(1.10)
        cy.minZoom(0.35)
    
        handleNodeClicks(cy)
  }
  
  document.addEventListener('DOMContentLoaded', function() {
   
    initEventListeners();
    initCytoscape();
    
  }, false);