import { setAbsolutePosition, showUI } from "@create-figma-plugin/utilities";

export default function () {
  showUI({
    height: 574,
    width: 420,
    title: "UX ANNOTATOR",
    themeColors: false,
  });

  const userName = "umarrg";

  figma.root.setRelaunchData({ open: "" });

  function checkSelection() {
    if (figma.currentPage.selection.length === 0) {
      figma.ui.postMessage({
        type: "no-element-selected",
      });
    } else {
      figma.ui.postMessage({
        type: "element-selected",
        userName,
      });
    }
  }

  checkSelection();

  figma.on("selectionchange", () => {
    checkSelection();
  });

  function hexToRGB(hex: string) {
    hex = hex.slice(1);
    const value = parseInt(hex, 16);
    const r = ((value >> 16) & 255) / 255;
    const g = ((value >> 8) & 255) / 255;
    const b = (value & 255) / 255;
    return { r, g, b };
  }
  type listData = {
    title: string;
    subtitle: string;
    content: string;
    color: string;
    number: number;
    position: string;
    li: string;
    id: string;
    frame?: string;
  };
  const list: Array<listData> = [];
  const annotations: Array<listData> = [];
  const divList: Array<string> = [];
  let frame: any = null;
  function createAnnotation(
    title: string,
    subtitle: string,
    content: string,
    color: string,
    position: string,
    li: string,
    fr: string | undefined
  ) {
    let number = list.length;

    if (divList.indexOf(li) === -1) {
      divList.push(li);
    }

    const selection = figma.currentPage.selection;
    const parentFrame = selection[0];

    const selectedFrame = figma.currentPage.selection[0];

    function createFrame() {
      if (
        parentFrame.parent!.constructor.name === "PageNode" ||
        parentFrame.parent!.parent!.constructor.name === "PageNode"
      ) {
        frame = figma.createFrame();
        frame.x = selectedFrame.x + selectedFrame.width;
        frame.y = selectedFrame.y;

        frame.resize(522, selectedFrame.height);
        frame.horizontalPadding = 20;
        frame.verticalPadding = 20;
        frame.fills = [
          {
            type: "SOLID",
            color: hexToRGB("#EBEBEB"),
          },
        ];
        let yOffset = 80;
        divList.forEach((item, idx) => {
          // yOffset = item == "List 1" ? 80 : selectedFrame.height / 2 + 60;
          // console.log(divList.length);

          const line = figma.createLine();
          line.x = selectedFrame.x + selectedFrame.width;
          line.y = selectedFrame.y + 35;
          line.resize(200, 0);
          line.strokeWeight = 3;
          line.strokes = [
            {
              type: "SOLID",
              color: hexToRGB("#BCBDBE"),
              opacity: 1,
            },
          ];
          const rec = figma.createRectangle();
          rec.resize(430, 40);
          rec.x = 200;
          rec.y = 15;
          rec.fills = [{ type: "SOLID", color: hexToRGB("#BCBDBE") }];
          rec.topLeftRadius = 20;
          rec.topRightRadius = 0;
          rec.bottomLeftRadius = 20;
          rec.bottomRightRadius = 0;
          const textNode = figma.createText();
          textNode.x = rec.x + 25;
          textNode.y = rec.y + 5;
          textNode.characters = item;
          textNode.fontName = {
            family: "Roboto Condensed",
            style: "Regular",
          };
          textNode.fontSize = 22;
          textNode.fills = [{ type: "SOLID", color: hexToRGB("#545454") }];
          frame.appendChild(rec);
          frame.appendChild(textNode);
        });

        list.forEach((data, idx) => {
          console.log("s", data.frame, "d", selectedFrame.parent?.name);
          if (
            data.id == selectedFrame.id ||
            data.frame == selectedFrame.parent?.name
          ) {
            const ellipseWidth = 36;
            const ellipseHeight = 36;
            // Create an ellipse
            const ellipse = figma.createEllipse();
            ellipse.x = 35;
            ellipse.y = yOffset;
            ellipse.fills = [{ type: "SOLID", color: hexToRGB(data.color) }];
            ellipse.strokes = [
              {
                type: "SOLID",
                color: hexToRGB("#FFFFFF"),
                opacity: 1, // Set the opacity
              },
            ];
            ellipse.strokeWeight = 2;
            ellipse.resize(ellipseWidth, ellipseHeight);
            let num = idx + 1;
            const text = figma.createText();
            text.characters = num.toString(); // Convert number to string
            text.x = ellipse.x + ellipseWidth / 3;
            text.y = ellipse.y + ellipseHeight / 6;
            text.fontName = { family: "Open Sans", style: "Regular" };
            text.fontSize = 20;

            text.fills = [{ type: "SOLID", color: hexToRGB("#FFFFFF") }]; // Set text color

            // Create the title
            const title = figma.createText();
            title.characters = data.title;
            title.x = 80;
            title.y = yOffset;
            title.fontSize = 16;
            title.fontName = { family: "Roboto", style: "Regular" };
            title.fontSize = 24;
            title.fills = [{ type: "SOLID", color: hexToRGB("#242424") }];

            const subtitle = figma.createText();
            subtitle.characters = data.subtitle;
            subtitle.x = 80;
            subtitle.y = yOffset + 24;
            subtitle.fontName = { family: "Roboto", style: "Regular" };
            subtitle.fontSize = 16;
            subtitle.fills = [{ type: "SOLID", color: hexToRGB("#242424") }];

            const content = figma.createText();
            content.characters = data.content;
            content.x = 80;
            content.y = yOffset + 50;
            content.fontName = { family: "Roboto", style: "Light" };
            content.fontSize = 14;
            content.fills = [{ type: "SOLID", color: hexToRGB("#242424") }];

            const divider = figma.createLine();
            divider.x = 50;
            divider.resize(386, 0);
            divider.y = yOffset + 70;
            divider.strokeWeight = 1;
            divider.strokes = [
              {
                type: "SOLID",
                color: hexToRGB("#B3B3B3"),
                opacity: 1,
              },
            ];

            yOffset += 80;

            frame.appendChild(ellipse);
            frame.appendChild(text);
            frame.appendChild(title);
            frame.appendChild(subtitle);
            frame.appendChild(content);
            frame.appendChild(divider);
          }
        });
      } else {
        console.log("select a frame first");
      }
    }

    // if (
    //   selectedFrame.type == "FRAME" ||
    //   selectedFrame.type == "RECTANGLE" ||
    //   selectedFrame.type == "COMPONENT"
    // ) {
    //   createFrame();
    // }
    createFrame();

    function createEllipse(position: string, number: number, color: string) {
      const selectedNode = figma.currentPage.selection[0];
      const ellipse = figma.createEllipse();
      ellipse.resize(40, 40);
      const text = figma.createText();

      // Set the text properties
      text.fontName = { family: "Open Sans", style: "Regular" };
      text.characters = idx.toString();
      text.fontSize = 24;
      text.fills = [{ type: "SOLID", color: hexToRGB("#FFFFFF") }];

      // Set the position of the ellipse and text based on the given position argument
      switch (position) {
        case "top":
          ellipse.x = selectedNode.x + selectedNode.width / 2;
          ellipse.y = selectedNode.y - 20;
          text.x = selectedNode.x + selectedNode.width / 2 + 6;
          text.y = selectedNode.y - 18;
          break;
        case "bottom":
          ellipse.x = selectedNode.x + selectedNode.width / 2;
          ellipse.y = selectedNode.y + selectedNode.height;
          text.x = selectedNode.x + selectedNode.width / 2 + 6;
          text.y = selectedNode.y + selectedNode.height + 7;
          break;
        case "left":
          ellipse.x = selectedNode.x - 25;
          ellipse.y = selectedNode.y + selectedNode.height / 2;
          text.x = selectedNode.x - 15;
          text.y = selectedNode.y + selectedNode.height / 2 + 2;
          break;
        case "right":
          ellipse.x = selectedNode.x + selectedNode.width;
          ellipse.y = selectedNode.y + selectedNode.height / 2;
          text.x = selectedNode.x + selectedNode.width + 8;
          text.y = selectedNode.y + selectedNode.height / 2 + 2;
          break;
        default:
          figma.notify(
            "Invalid position argument. Use 'top', 'bottom', 'left', or 'right'."
          );
          return;
      }

      // Set the color and stroke properties of the ellipse
      ellipse.fills = [{ type: "SOLID", color: hexToRGB(color) }];
      ellipse.strokeWeight = 0;

      // Add the pointer triangle at the top of the ellipse
      const triangle = figma.createPolygon();
      triangle.resize(15, 15);
      triangle.rotation = 180;
      triangle.points = [
        { x: 0, y: 0 },
        { x: 15, y: 7.5 },
        { x: 0, y: 15 },
      ];
      triangle.fills = [{ type: "SOLID", color: hexToRGB(color) }];
      triangle.x = selectedNode.x + selectedNode.width / 2;
      triangle.y = selectedNode.y - 20;

      // Add the ellipse and text to the parent of the selected node
      selectedNode.parent.appendChild(ellipse);
      selectedNode.parent.appendChild(text);
      selectedNode.parent.appendChild(triangle);

      // Scroll and zoom into view
      figma.viewport.scrollAndZoomIntoView([ellipse]);
    }

    function createPin(position: string, number: number, color: string) {
      const selectedNode = figma.currentPage.selection[0];
      const text = figma.createText();
      text.characters = number.toString();
      text.fontName = { family: "Open Sans", style: "Regular" };
      text.fontSize = 18;
      text.fills = [{ type: "SOLID", color: hexToRGB("#F44336") }];

      const existingNode = figma.createNodeFromSvg(
        `<svg width="52" height="75" viewBox="0 0 52 75" fill="none" xmlns="http://www.w3.org/2000/svg">
<path d="M25.5262 2.213e-06C39.6018 2.213e-06 51.0522 11.4505 51.0522 25.5261C51.0522 30.9721 49.7426 36.9141 47.1581 43.1912C42.6806 54.0653 34.2241 65.8513 26.3471 73.9182C25.8946 74.3707 25.1574 74.3703 24.7053 73.9182C16.8385 65.8639 8.37415 54.0708 3.89432 43.1912C1.30982 36.9141 0.000156403 30.9721 0.000156403 25.5261C0.000156403 11.4505 11.4505 2.213e-06 25.5262 2.213e-06Z" fill="#F44336"/>
<path d="M25.7608 8C26.3425 8 26.9227 8.02851 27.5016 8.08552C28.0805 8.14254 28.6552 8.22779 29.2257 8.34127C29.7962 8.45475 30.3598 8.59592 30.9164 8.76477C31.4731 8.93363 32.0201 9.12936 32.5575 9.35196C33.0949 9.57456 33.6201 9.82296 34.1331 10.0972C34.6461 10.3714 35.1445 10.6701 35.6281 10.9932C36.1118 11.3164 36.5784 11.6625 37.0281 12.0315C37.4777 12.4005 37.9082 12.7907 38.3195 13.202C38.7308 13.6133 39.121 14.0438 39.49 14.4935C39.859 14.9431 40.2051 15.4098 40.5283 15.8934C40.8515 16.3771 41.1502 16.8754 41.4244 17.3884C41.6986 17.9014 41.947 18.4266 42.1696 18.964C42.3922 19.5014 42.5879 20.0484 42.7568 20.6051C42.9256 21.1617 43.0668 21.7253 43.1803 22.2958C43.2937 22.8663 43.379 23.441 43.436 24.0199C43.493 24.5988 43.5215 25.1791 43.5215 25.7608C43.5215 26.3424 43.493 26.9227 43.436 27.5016C43.379 28.0805 43.2937 28.6552 43.1803 29.2257C43.0668 29.7962 42.9256 30.3598 42.7568 30.9164C42.5879 31.4731 42.3922 32.0201 42.1696 32.5575C41.947 33.0949 41.6986 33.6201 41.4244 34.1331C41.1502 34.6461 40.8515 35.1445 40.5283 35.6281C40.2051 36.1118 39.859 36.5784 39.49 37.0281C39.121 37.4777 38.7308 37.9082 38.3195 38.3195C37.9082 38.7308 37.4777 39.121 37.0281 39.49C36.5784 39.859 36.1118 40.2051 35.6281 40.5283C35.1445 40.8515 34.6461 41.1502 34.1331 41.4244C33.6201 41.6986 33.0949 41.947 32.5575 42.1696C32.0201 42.3922 31.4731 42.5879 30.9164 42.7567C30.3598 42.9256 29.7962 43.0668 29.2257 43.1803C28.6552 43.2937 28.0805 43.379 27.5016 43.436C26.9227 43.493 26.3425 43.5215 25.7608 43.5215C25.1791 43.5215 24.5988 43.493 24.0199 43.436C23.441 43.379 22.8663 43.2937 22.2958 43.1803C21.7253 43.0668 21.1617 42.9256 20.6051 42.7567C20.0484 42.5879 19.5014 42.3922 18.964 42.1696C18.4266 41.947 17.9014 41.6986 17.3884 41.4244C16.8754 41.1501 16.3771 40.8515 15.8934 40.5283C15.4098 40.2051 14.9431 39.859 14.4935 39.49C14.0438 39.121 13.6133 38.7308 13.202 38.3195C12.7907 37.9082 12.4005 37.4777 12.0315 37.0281C11.6625 36.5784 11.3164 36.1118 10.9932 35.6281C10.6701 35.1445 10.3714 34.6461 10.0972 34.1331C9.82296 33.6201 9.57456 33.0949 9.35196 32.5575C9.12936 32.0201 8.93363 31.4731 8.76477 30.9164C8.59592 30.3598 8.45475 29.7962 8.34127 29.2257C8.22779 28.6552 8.14254 28.0805 8.08552 27.5016C8.02851 26.9227 8 26.3424 8 25.7608C8 25.1791 8.02851 24.5988 8.08552 24.0199C8.14254 23.441 8.22779 22.8663 8.34127 22.2958C8.45475 21.7253 8.59592 21.1617 8.76477 20.6051C8.93363 20.0484 9.12936 19.5014 9.35196 18.964C9.57456 18.4266 9.82296 17.9014 10.0972 17.3884C10.3714 16.8754 10.6701 16.3771 10.9932 15.8934C11.3164 15.4098 11.6625 14.9431 12.0315 14.4935C12.4005 14.0438 12.7907 13.6133 13.202 13.202C13.6133 12.7907 14.0438 12.4005 14.4935 12.0315C14.9431 11.6625 15.4098 11.3164 15.8934 10.9932C16.3771 10.6701 16.8754 10.3714 17.3884 10.0972C17.9014 9.82296 18.4266 9.57456 18.964 9.35196C19.5014 9.12936 20.0484 8.93363 20.6051 8.76477C21.1617 8.59592 21.7253 8.45475 22.2958 8.34127C22.8663 8.22779 23.441 8.14254 24.0199 8.08552C24.5988 8.02851 25.1791 8 25.7608 8Z" fill="white"/>
 

</svg>
`
      );

      // text.x = existingNode.x + existingNode.width / 2.5;
      // text.y = existingNode.y + existingNode.height / 6;
      switch (position) {
        case "top":
          existingNode.x = selectedNode.x + selectedNode.width / 2;
          existingNode.y = selectedNode.y - 20;
          text.x = selectedNode.x + selectedNode.width / 2 + 20;
          text.y = selectedNode.y - 8;
          break;
        case "bottom":
          existingNode.x = selectedNode.x + selectedNode.width / 2;
          existingNode.y = selectedNode.y + selectedNode.height;
          text.x = selectedNode.x + selectedNode.width / 2 + 20;
          text.y = selectedNode.y + selectedNode.height + 14;
          break;
        case "left":
          existingNode.x = selectedNode.x - 25;
          existingNode.y = selectedNode.y + selectedNode.height / 2;
          text.x = selectedNode.x - 7;
          text.y = selectedNode.y + selectedNode.height / 2 + 12;
          break;
        case "right":
          existingNode.x = selectedNode.x + selectedNode.width;
          existingNode.y = selectedNode.y + selectedNode.height / 2;
          text.x = selectedNode.x + selectedNode.width + 20;
          text.y = selectedNode.y + selectedNode.height / 2 + 10;
          break;
        default:
          figma.notify(
            "Invalid position argument. Use 'top', 'bottom', 'left', or 'right'."
          );
          return;
      }
      selectedNode.parent!.appendChild(existingNode);
      selectedNode.parent!.appendChild(text);

      // figma.currentPage.appendChild(existingNode);
      // figma.currentPage.appendChild(text);
      figma.viewport.scrollAndZoomIntoView([existingNode, text]);
    }
    createPin(position, number, color);
  }

  figma.ui.onmessage = async (message) => {
    if (message.type === "add-annotation") {
      console.log("CODE LOG", message);

      const fontsToLoad = [
        { family: "Roboto", style: "Regular" },
        { family: "Roboto", style: "Light" },
        { family: "Inter", style: "Regular" },
        { family: "Open Sans", style: "Regular" },
        { family: "Roboto Condensed", style: "Regular" },
      ];

      await Promise.all(
        fontsToLoad.map(async (font) => {
          await figma
            .loadFontAsync(font)
            .then(() => {
              // createAnnotation(message.position, message.title, message.content);
            })
            .catch((err) => console.log(err));
        })
      );
      const selectedObject = figma.currentPage.selection[0];
      if (message.position && message.title) {
        if (selectedObject) {
          list.push({
            title: message.title,
            content: message.content,
            color: message.bgColor,
            subtitle: message.subtitle,
            number: annotations.length,
            position: message.position,
            li: "FUNCTIONAL ANNOTATIONS",
            id: selectedObject.id,
            frame: selectedObject?.parent?.name,
          });

          createAnnotation(
            message.title,
            message.subtitle,
            message.content,
            message.bgColor,
            message.position,
            message.list,
            selectedObject?.parent?.name
          );
        } else {
          figma.notify("Please Select a object or frame");
        }
      }
    }
  };
}