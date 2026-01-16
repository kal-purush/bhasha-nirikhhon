import Graph from "./Graph.js"
import GraphRenderer from "./GraphRenderer.js";

export default class GraphManipulator {
    #graph
    #renderer
    #container

    constructor() {
        this.#graph = new Graph();
        this.#renderer = new GraphRenderer();
        this.#container = document.createElement("div");
        this.#container.classList.add("graph", "manipulator");
        this.#container.replaceChildren(this.setupInput(), this.#renderer.canvas);
    }

    setupInput() {
        const inputPanel = document.createElement("section")
        const addNodeBtn = document.createElement("input");
        addNodeBtn.defaultValue = "add node";
        addNodeBtn.addEventListener("dblclick", () => {
            // this.#graph.addNode(addNodeBtn.value);
            // addNodeBtn.value = addNodeBtn.defaultValue;
            (requestAnimationFrame.bind(this))(this.#renderer.updateView(this.#graph.nodes, this.#graph.edges));
        })
        const removeNodeBtn = document.createElement("input");
        removeNodeBtn.defaultValue = "remove node";
        const addEdgeBtn = document.createElement("input");
        addEdgeBtn.defaultValue = "add edge";
        const removeEdgeBtn = document.createElement("input");
        removeEdgeBtn.defaultValue = "remove edge";
        inputPanel.replaceChildren(addNodeBtn, removeNodeBtn, addEdgeBtn, removeEdgeBtn);
        return inputPanel;
    }

    get container() {
        return this.#container;
    }
    
    get graph() {
        return this.#graph;
    }

    reset() {
        this.#graph.clear();
    }
}