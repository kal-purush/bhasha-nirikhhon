

export const convertKeysToCamelCase = (obj: any): any => {
    if (Array.isArray(obj)) {
      return obj.map((item) => convertKeysToCamelCase(item));
    } else if (
      typeof obj === "object" &&
      obj !== null &&
      !(obj instanceof Date)
    ) {
      const camelCaseObj: any = {};
      for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
          const camelCaseKey = key.replace(/[^a-zA-Z0-9]+(.)/g, (m, chr) => {
            return chr.toUpperCase();
          });
          camelCaseObj[camelCaseKey] = convertKeysToCamelCase(obj[key]);
        }
      }
      return camelCaseObj;
    } else {
      return obj;
    }
  };``
  
  export const convertKeysToSnakeCase = (obj: any, preserveFields?: any): any => {
    if (Array.isArray(obj)) {
      return obj.map((item) => convertKeysToSnakeCase(item, preserveFields));
    } else if (
      typeof obj === "object" &&
      obj !== null &&
      !(obj instanceof Date)
    ) {
      const snakeObject: any = {};
      for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
          const snakeKey = key.replace(/([A-Z])/g, "_$1").toLowerCase();
          if (
            (key === "createdAt" || key === "updatedAt") &&
            typeof obj[key] === "string"
          ) {
            // Convert createdAt to UTC format if it's a string
            snakeObject[key] = new Date(obj[key]).toISOString();
          } else if (preserveFields?.includes(key)) {
            snakeObject[key] = obj[key];
          } else {
            snakeObject[snakeKey] = convertKeysToSnakeCase(
              obj[key],
              preserveFields
            );
          }
        }
      }
      return snakeObject;
    } else {
      return obj;
    }
  };