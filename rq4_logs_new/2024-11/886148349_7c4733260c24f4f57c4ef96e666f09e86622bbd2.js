AFRAME.registerComponent("add-button", {
  schema: {
    setToggle: { type: "boolean" },
  },
  init: function () {
    this.toggle = this.data.setToggle;

    this.el.addEventListener("click", (event) => {
      if (event.detail && event.detail.cursorEl) {
        return;
      }

      const spawnPos = "0 1.5 0";
      const gate = this.el.querySelector("a-text").getAttribute("value");
      const inputPlate = `
       <a-box
        shadow="cast:true;receive:false"
        added
        input-logic
        logic-gate
        class="interactable gates"
        dynamic-body
        grabbable
        hoverable
        material="color: white; wireframe: true; opacity: 0;visible:false"
        depth="0.1 "
        height="0.15"
        width="0.28"
        rotation="-90 0 0"
        position="${spawnPos}"
      >
        <!--position="-0.038  0.041 -0.02" -->
        <a-gltf-model 
          hoverable
          button
          src="#Button"
          position="-0.086  0.041 -0.02"  
          rotation="180 0 0"
          scale="0.008 0.008 0.005"
        >
            
     
          <a-text
            value="Input"
            color="yellow"
            rotation="180 0 0"
            position="-0.368 4.354 -7.602"
            scale="20 20 20"
          ></a-text
        ></a-gltf-model>

        <a-gltf-model
          input-button
          src="#Button_case"
          position="-0.11 0.06 -0.02"
          rotation="180 0 0"
          scale="0.01 0.01 0.01"
        >
        </a-gltf-model>

        <a-box
          position="0.05 0 -0.01"
          height="0.025"
          width="0.1"
          depth="0.02"
          material="color: black"
        >
        </a-box>

        <a-box
          hoverable
          draggable
          droppable
          logic-node
          transmit-node="value:true"
          opacity="0"
          class="nodes transmit-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="0.1 0 0"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere color="blue" scale="0.45 0.45 0.45"></a-sphere>
        </a-box>
      </a-box>`;
      const notPlate = `
        <a-box
        shadow="cast:true;receive:false"
        added
        not-logic
        logic-gate
        class="interactable gates"
        dynamic-body
      grabbable
        hoverable
        material="color: white; wireframe: true;opacity:0;visible:false"
        depth="0.05"
        height="0.3"
        width="0.6"
        rotation="-90 0 0"
        position="${spawnPos}"
      >
        <a-gltf-model
          position="-0.3 -0.15 0"
          src="#NOT_GATE_PLATE"
          scale="0.018 0.02 0.04"
          rotation="0 0 0"
          material="color:red"
        ></a-gltf-model>
        <a-text
          value="NOT"
          color="yellow"
          scale="0.4 0.4 0.4"
          rotation="0 0 0"
          position="-0.18 0 0"
        ></a-text>
        <a-box
          droppable
          logic-node
          recieve-node
          opacity="0"
          class="nodes recieve-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.3 0 -0.02"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="red"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
        <a-box
          hoverable
          draggable
          droppable
          logic-node
          transmit-node
          opacity="0"
          class="nodes transmit-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="0.26 0 -0.02"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="blue"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
      </a-box>`;
      const orPlate = `
      
        <a-box
        shadow="cast:true;receive:false"
        added
        or-logic
        logic-gate
        class="interactable gates"
        dynamic-body
        grabbable
        hoverable
        material="color: white; wireframe: true;opacity:0;visible:false"
        depth="0.05"
        height="0.3"
        width="0.6"
        rotation="-90 0 0"
        position="${spawnPos}"
      >
        <a-gltf-model
          position="-0.3 -0.15 0"
          src="#OR_GATE_PLATE"
          scale="0.02 0.02 0.04"
          rotation="0 0 0"
          material="color:red"
        ></a-gltf-model>
        <a-text
          value="OR"
          color="yellow"
          scale="0.5 0.5 0.5"
          rotation="0 0 0"
          position="-0.16 0 0"
        ></a-text>
        <a-box
          droppable
          logic-node
          recieve-node
          opacity="0"
          class="nodes recieve-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.3 0.08 -0.02"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="red"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
        <a-box
          droppable
          logic-node
          recieve-node
          class="nodes recieve-node"
          opacity="0"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.3 -0.08 -0.02"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="red"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
        <a-box
          hoverable
          draggable
          droppable
          logic-node
          transmit-node
          opacity="0"
          class="nodes transmit-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="0.23 -0 -0.02"
        >
        <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="blue"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
      </a-box>`;
      const andPlate = `
     
      <a-box
        shadow="cast:true;receive:false"
        added
        and-logic
        logic-gate
        class="interactable gates"
        dynamic-body
       grabbable
        hoverable
        material="color: white; wireframe: true;opacity:0;visible:false"
        depth="0.05"
        height="0.3"
        width="0.6"
        rotation="-90 0 0"
        position="${spawnPos}"
      >
        <a-gltf-model
          position="-0.3 -0.15 0"
          src="#AND_GATE_PLATE"
          scale="0.02 0.02 0.04"
          rotation="0 0 0"
          material="color:red"
        ></a-gltf-model>
        <a-text
          value="AND"
          color="yellow"
          scale="0.5 0.5 0.5"
          rotation="0 0 0"
          position="-0.16 0 0"
        ></a-text>
        <a-box
          droppable
          logic-node
          recieve-node
          opacity="0"
          class="nodes recieve-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.3 0.08 -0.02"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="red"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
        <a-box
          droppable
          logic-node
          recieve-node
          opacity="0"
          class="nodes recieve-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.3 -0.08 -0.02"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="red"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
        <a-box
          hoverable
          draggable
          droppable
          logic-node
          transmit-node
          opacity="0"
          class="nodes transmit-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="0.23 -0 -0.02"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere
            color="blue"
            scale="0.45 0.45 0.45"
          ></a-sphere>
        </a-box>
      </a-box>`;
      const xorPlate = `

      <a-box
        shadow="cast:true;receive:false"
        added
        xor-logic
        logic-gate
        class="interactable gates"
        dynamic-body
        grabbable
        hoverable
        material="color: white; wireframe: true;opacity:0;visible:false"
        depth="0.05"
        height="0.3"
        width="0.6"
        rotation="-90 0 0"
        position="${spawnPos}"
      >
        <a-gltf-model
          position="-0.3 -0.15 0"
          src="#XOR_GATE_PLATE"
          scale="0.02 0.02 0.04"
          rotation="0 0 0"
          material="color:red"
        ></a-gltf-model>
        <a-text
          value="XOR"
          color="yellow"
          scale="0.5 0.5 0.5"
          rotation="0 0 0"
          position="-0.16 0 0"
        ></a-text>
        <a-box
          droppable
          logic-node
          recieve-node
          class="nodes recieve-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.3 0.08 -0.02"
          opacity="0"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere color="red" scale="0.45 0.45 0.45"></a-sphere>
        </a-box>
        <a-box
          droppable
          logic-node
          recieve-node
          class="nodes recieve-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.3 -0.08 -0.02"
          opacity="0"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere color="red" scale="0.45 0.45 0.45"></a-sphere>
        </a-box>
        <a-box
          hoverable
          draggable
          droppable
          logic-node
          transmit-node
          class="nodes transmit-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="0.27 -0 -0.02"
          opacity="0"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere color="blue" scale="0.45 0.45 0.45"></a-sphere>
        </a-box>
      </a-box>`;
      const outputPlate = `
            <a-box
        shadow="cast:true;receive:false"
        output-logic
        logic-gate
        class="interactable gates"
        dynamic-body
        grabbable
        hoverable
        material="color: white; wireframe: true; opacity: 0;visible:false"
        depth="0.1 "
        height="0.15"
        width="0.28"
        rotation="-90 0 0"
        position="${spawnPos}"
      >
        <a-gltf-model
          output-button
          src="#Button"
          position="0.016 0.041 -0.02"
          rotation="180 0 0"
          scale="0.008 0.008 0.005"
        >
          <a-text
            value="Output"
            color="yellow"
            rotation="180 0 0"
            position="-0.368 4.354 -7.602"
            scale="20 20 20"
          ></a-text
        ></a-gltf-model>
        <a-gltf-model
          src="#Button_case"
          position="-0.006 0.06 -0.02"
          rotation="180 0 0"
          scale="0.01 0.01 0.01"
        >
        </a-gltf-model>

        <a-box
          position="-0.05 0 -0.01"
          height="0.025"
          width="0.1"
          depth="0.02"
          material="color: black"
        >
        </a-box>

        <a-box
          droppable
          logic-node
          recieve-node
          opacity="0"
          class="nodes recieve-node"
          color="white"
          scale="0.07 0.07 0.07"
          wireframe="true"
          position="-0.1 0 0"
        >
          <a-text position="-0.5 0 1" rotation="90 0 0" scale="5 5 5 "></a-text>
          <a-sphere id="output_node" color="white" scale="0.45 0.45 0.45"></a-sphere>
        </a-box>
      </a-box>
`;

      const sceneEl = document.querySelector("a-scene");
      const container = document.createElement("a-entity");
      container.setAttribute("position", "-1 1 -2");
      switch (gate) {
        case "Input":
          container.innerHTML = inputPlate;
          break;
        case "NOT":
          container.innerHTML = notPlate;
          break;
        case "OR":
          container.innerHTML = orPlate;
          break;
        case "AND":
          container.innerHTML = andPlate;
          break;
        case "XOR":
          container.innerHTML = xorPlate;
          break;
        case "Output":
          container.innerHTML = outputPlate;
          break;
        default:
          console.warn(`Unknown gate type: ${gate}`);
          return;
      }

      const addedEl = container.querySelector("[added]");
      sceneEl.appendChild(container);
      // grabbable component cant be dynamically off dunno why

      // setTimeout(() => {
      //   if (this.toggle) {
      //     // Remove grabbable component using proper A-Frame method
      //     addedEl.removeAttribute("grabbable");
      //     // Also remove the component system
      //     // addedEl.components.grabbable.remove();
      //     // Force component cleanup

      //   }
      // }, 0);
      addedEl.removeAttribute("added");
    });
  },
  update: function () {
    this.toggle = this.data.setToggle;
  },
});