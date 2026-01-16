// Script which generates all the helper methods
// inside net/forthecrown/nbt/ListTag
//
// Note: This script isn't full do-it-all type thing, this will require
// cleanup after the output has been generated
//

const types = [
  { type: "String",     name: "String",     to_tag: "stringTag" },
  { type: "byte",       name: "Byte" },
  { type: "short",      name: "Short" },
  { type: "int",        name: "Int" },
  { type: "long",       name: "Long" },
  { type: "double",     name: "Double" },
  { type: "float",      name: "Float" },
  { type: "byte...",    name: "ByteArray" },
  { type: "int...",     name: "IntArray" },
  { type: "long...",    name: "LongArray" },
  { type: "double...",  name: "DoubleArray" },
  { type: "float...",   name: "FloatArray" },
  { type: "String...",  name: "StringArray", to_tag: "stringList" },
];

// Output string
let string = "// Type-specific values";

for (var {type, name, to_tag} of types) {
  let toTag = to_tag == undefined ? toTagFunction(type) : to_tag;

  printFunctions(name, type, toTag, true);

  let isArray = isArrayType(type);

  // Arrays generate 2 types of method, one with type[] and one with List<Type>
  if (isArray) {
    let first = type.substring(0, 1);
    let bareName = type.substring(0, type.length - 3);
    let typeName = first.toUpperCase() + (bareName == "int" ? "nteger" : bareName.substring(1));

    printFunctions(name, `List<${typeName}>`, toTag, false);
  }
}

console.log(string);

function printFunctions(typeName, type, functionName, makeGetter) {
  let getter;

  let isArray = isArrayType(type);
  let actualType = isArray ? type.substring(0, type.length - 3) + "[]" : type;
  let docType = actualType;

  if (makeGetter) {
    let bareType = isArray ? type.substring(0, type.length - 3) : type;

    let isNullable = isArray || bareType == "string" || bareType == "String";
    let titleCase = bareType.substring(0, 1).toUpperCase() + bareType.substring(1);
    let lowerCase = titleCase.substring(0, 1).toLowerCase() + titleCase.substring(1);

    let tagType;
    let tagClass;

    if (bareType == "byte" || bareType == "int" || bareType == "long") {
      if (isArray) {
          tagType = bareType + "ArrayType";
          tagClass = titleCase + "ArrayTag"
        } else {
          tagType = lowerCase + "Type";
          tagClass = titleCase + "Tag";
        }
    } else {
      if (isArray) {
          tagType = "listType"
          tagClass = "ListTag"
        } else {
          tagType = lowerCase + "Type";
          tagClass = titleCase + "Tag";
        }
    }

    let valueGetterFunction;

    if (isArray) {
      if (tagType == "listType") {
        valueGetterFunction = `toArray(new ${bareType}[tag.size()])`
      } else {
        valueGetterFunction = `to${titleCase}Array()`
      }
    } else {
      switch(bareType) {
        case "String":
          valueGetterFunction = "value()";
          break;

        default:
          valueGetterFunction = `${bareType}Value()`;
          break;
      }
    }

    let defValue = isNullable
        ? ((isArray ? `(${actualType}) ` : "") + "null")
        : (type == "short" || type == "byte" ? `(${type}) ` : "") + "0";

    // Omg this is horrendous, doesn't matter though, this is just a
    // small script

    getter = `/**
 * Gets a {@code ${docType}} value from the tag
 * @param index Element index
 * @return Found value, or {@code ${defValue}}, if the entry wasn't found
 */
default ${actualType} get${typeName}(int index) {
  return get${typeName}(index, ${defValue});
}

/**
 * Gets a {@code ${docType}} value from the tag
 * @param index Element index
 * @param defaultValue Default return value
 * @return Found value, or {@code defaultValue}, if the entry wasn't found
 */
default ${actualType} get${typeName}(int index, ${type} defaultValue) {
  if (!typeMatches(TagTypes.${tagType}()) || index < 0 || index >= size()) {
    return defaultValue;
  }

  ${tagClass} tag = get(index, TagTypes.${tagType}());
  return tag.${valueGetterFunction};
}`
  } else {
    getter = "";
  }

  string +=
`

/**
 * Adds a {@code ${docType}} value to the list
 * @param value Value to add
 * @return {@code true}, if the value was inserted into the list, {@code false} otherwise
 */
default boolean add${typeName}(${type} value) {
  var tag = BinaryTags.${functionName}(value);
  return add(tag);
}

/**
 * Adds a {@code ${docType}} value to the list
 * @param index Index to add the value at
 * @param value Value to add
 */
default void add${typeName}(int index, ${type} value) {
  var tag = BinaryTags.${functionName}(value);
  add(index, tag);
}

/**
 * Sets a value inside this list
 * @param index Index to set
 * @param value Value to set
 */
default void set${typeName}(int index, ${type} value) {
  var tag = BinaryTags.${functionName}(value);
  set(index, tag);
}

${getter}`;
}

function isArrayType(type) {
  return type.endsWith("...");
}

function toTagFunction(typeName) {
  let isArray = false;

  if (typeName.endsWith("...")) {
    typeName = typeName.substring(0, typeName.length - 3);
    isArray = true;
  }

  if (isArray) {
    switch(typeName) {
      case "byte":
        return "byteArrayTag";

      case "int":
        return "intArrayTag"

      case "long":
        return "longArrayTag"

      default:
        return typeName + "List"
    }
  }

  return typeName + "Tag";
}