var container1 = document.getElementById("mynetwork");
var container2 = document.getElementById("mynetwork2");
var genNew = document.getElementById("generate-graph");
var solve = document.getElementById("solve");
var temptext = document.getElementById("temptext");

var NODES;
var netArray;
var curr_data;

var options = {
  edges: {
    font: {
      size: 20,
    },
    arrows: {
      to: {
        enabled: true,
      },
    },
  },
  nodes: {
    font: "12px arial red",
    scaling: {
      label: true,
    },
    shape: "icon",
    icon: {
      face: "FontAwesome",
      code: "\uf183",
      size: 50,
      color: "#991133",
    },
  },
};

var network1 = new vis.Network(container1);
network1.setOptions(options);

var network2 = new vis.Network(container2);
network2.setOptions(options);

function createData() {
  //generate random no bw 3 to 11
  NODES = Math.floor(Math.random() * 9) + 3;
  let nodes_array = [];
  for (let i = 1; i <= NODES; i++) {
    nodes_array.push({ id: i, label: `Person ${i}` });
  }
  var nodes = new vis.DataSet(nodes_array);

  let edges_array = [];
  netArray = Array(NODES).fill(0);
  for (let i = 2; i <= NODES; i++) {
    let neigh1 = Math.floor(Math.random() * (i - 1) + 1);
    let amt1 = Math.floor(Math.random() * 50) + 20;
    netArray[i - 1] -= amt1;
    netArray[neigh1 - 1] += amt1;
    edges_array.push({ from: i, to: neigh1, label: amt1.toString() });
    if (i != NODES) {
      let neigh2 = Math.floor(Math.random() * (NODES - i - 1) + i + 1);
      let amt2 = Math.floor(Math.random() * 50) + 20;
      netArray[i - 1] -= amt2;
      netArray[neigh2 - 1] += amt2;
      edges_array.push({ from: i, to: neigh2, label: amt2.toString() });
    }
  }
  var edges = new vis.DataSet(edges_array);

  var data = {
    nodes: nodes,
    edges: edges,
  };
  curr_data = data;
  network1.setData(data);
}
genNew.onclick = () => {
  createData();
  solve.disabled = false;
  temptext.style.display = "inline";
  container2.style.display = "none";
};

solve.onclick = () => {
  temptext.style.display = "none";
  container2.style.display = "inline";
  let solvedData = solveData();
  network2.setData(solvedData);
  solve.disabled = true;
};
function indexOfSmallest(a) {
  var lowest = 0;
  for (var i = 1; i < a.length; i++) {
    if (a[i] < a[lowest]) lowest = i;
  }
  return lowest;
}

function indexOfLargest(a) {
  var largest = 0;
  for (var i = 1; i < a.length; i++) {
    if (a[i] > a[largest]) largest = i;
  }
  return largest;
}
function solveData() {
  let edges_array = [];
  while (1) {
    let lowest = indexOfSmallest(netArray);
    let largest = indexOfLargest(netArray);
    if (netArray[lowest] == 0 && netArray[largest] == 0) {
      break;
    }
    let minAmount = Math.min(Math.abs(netArray[lowest]), netArray[largest]);
    netArray[lowest] += minAmount;
    netArray[largest] -= minAmount;
    edges_array.push({
      from: lowest + 1,
      to: largest + 1,
      label: minAmount.toString(),
    });
  }
  var edges = new vis.DataSet(edges_array);
  var data = {
    nodes: curr_data["nodes"],
    edges,
  };
  return data;
}

createData();