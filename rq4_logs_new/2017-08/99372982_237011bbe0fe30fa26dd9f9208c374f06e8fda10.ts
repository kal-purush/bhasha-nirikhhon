export class Search {
  public static search(targetElement: any, searchPhrase: string, searchMask: any) {
    let match = searchPhrase === "";
    if (!match) {
      for (const prop in targetElement) {
        if (
            targetElement.hasOwnProperty(prop) &&
            searchMask.hasOwnProperty(prop)
        ) {
          if (typeof targetElement[prop] === "object") {
            match = Search.search(
                targetElement[prop],
                searchPhrase,
                searchMask[prop]
            );
          } else {
            match = ((targetElement[prop] + "").toLowerCase().indexOf(searchPhrase.toLowerCase()) !== -1);
          }
          if (match) {
            break;
          }
        }
      }
    }
    return match;
  }
}