let keyMappings = {
  control: "C",
  meta: "M",
  shift: "S",
};
export function setKeyMappings(mappings) {
  keyMappings = mappings;
}

const skipKey = ["Control", "Meta", "Alt", "Shift"];
export function calcKeyStrByEvent(event) {
  const charKey = event.key.toLowerCase();
  if (skipKey.includes(charKey)) {
    return null;
  }

  const fnKeys = [];
  if (event.ctrlKey) {
    fnKeys.push(keyMappings.control);
  }
  if (event.metaKey) {
    fnKeys.push(keyMappings.meta);
  }
  if (event.shiftKey) {
    fnKeys.push(keyMappings.shift);
  }

  fnKeys.sort();
  return [...fnKeys, charKey].join("-");
}

export function formatKey(str) {
  const keys = str.split("-");
  const charKey = keys.pop();
  const fnKeys = keys;

  if (!charKey) {
    throw new Error(`format key failed, str is ${str}`);
  }

  fnKeys.sort();
  return [...fnKeys, charKey].join("-");
}