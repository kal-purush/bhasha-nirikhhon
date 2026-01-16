/*

Stripped-down and adapted Rangy library

Original: https://github.com/timdown/rangy/ (MIT license)

Ported because

- we only need modern browsers due to requiring the CSS Highlight API
  (the original works to support IE6+, which we don't need)
- there's a lot of other code we're not using
- we want to use Typescript and the standard Web API as much as possible
- the original Rangy loads slowly, and we don't want to have to defer
  loading the Highlights system

Need to port:

- checksum = rangy.getElementChecksum(el)
- range = rangy.deserializeRange(serialized, containerEl)
- ranges = selection.getAllRanges()
- selection.isCollapsed
- rangy.dom.isAncestorOf(el, range.commonAncestorContainer)
- selection = rangy.getSelection
- serialized = rangy.serializeSelection(selection, false [no checksum], containerEl)

*/

/* From rangy/lib/range-serializer.js */

const crc32 = (function () {
  function utf8encode(str: string) {
    var utf8CharCodes = [];

    for (var i = 0, len = str.length, c; i < len; ++i) {
      c = str.charCodeAt(i);
      if (c < 128) {
        utf8CharCodes.push(c);
      } else if (c < 2048) {
        utf8CharCodes.push((c >> 6) | 192, (c & 63) | 128);
      } else {
        utf8CharCodes.push(
          (c >> 12) | 224,
          ((c >> 6) & 63) | 128,
          (c & 63) | 128
        );
      }
    }
    return utf8CharCodes;
  }

  var cachedCrcTable: number[] | null = null;

  function buildCRCTable() {
    var table = [];
    for (var i = 0, j, crc; i < 256; ++i) {
      crc = i;
      j = 8;
      while (j--) {
        if ((crc & 1) == 1) {
          crc = (crc >>> 1) ^ 0xedb88320;
        } else {
          crc >>>= 1;
        }
      }
      table[i] = crc >>> 0;
    }
    return table;
  }

  function getCrcTable() {
    if (!cachedCrcTable) {
      cachedCrcTable = buildCRCTable();
    }
    return cachedCrcTable;
  }

  return function (str: string) {
    var utf8CharCodes = utf8encode(str),
      crc = -1,
      crcTable = getCrcTable();
    for (var i = 0, len = utf8CharCodes.length, y; i < len; ++i) {
      y = (crc ^ utf8CharCodes[i]) & 0xff;
      crc = (crc >>> 8) ^ crcTable[y];
    }
    return (crc ^ -1) >>> 0;
  };
})();

function escapeTextForHtml(str: string) {
  return str.replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function nodeToInfoString(node: Node, infoParts: string[] = []) {
  var nodeType = node.nodeType,
    children = node.childNodes,
    childCount = children.length;
  var nodeInfo = [nodeType, node.nodeName, childCount].join(":");
  var start = "",
    end = "";
  switch (nodeType) {
    case 3: // Text node
      start = escapeTextForHtml(node.nodeValue ?? "");
      break;
    case 8: // Comment
      start = "<!--" + escapeTextForHtml(node.nodeValue ?? "") + "-->";
      break;
    default:
      start = "<" + nodeInfo + ">";
      end = "</>";
      break;
  }
  if (start) {
    infoParts.push(start);
  }
  for (var i = 0; i < childCount; ++i) {
    nodeToInfoString(children[i], infoParts);
  }
  if (end) {
    infoParts.push(end);
  }
  return infoParts;
}

// Creates a string representation of the specified element's contents that is similar to innerHTML but omits all
// attributes and comments and includes child node counts. This is done instead of using innerHTML to work around
// IE <= 8's policy of including element properties in attributes, which ruins things by changing an element's
// innerHTML whenever the user changes an input within the element.
export function getElementChecksum(el: Element): string {
  var info = nodeToInfoString(el).join("");
  return crc32(info).toString(16);
}