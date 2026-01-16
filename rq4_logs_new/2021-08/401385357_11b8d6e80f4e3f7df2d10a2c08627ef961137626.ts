export default function jsonDeepEquals(
    [reference, ...items]: any[],
    debug: boolean = false,
    path: (string | number)[] = []
): boolean {

    const log = (...args: any[]) =>
        debug && console.log(`[json-deep-equals] ${path.join(".")} :`, ...args);

    // Tableau = itération élements
    if (Array.isArray(reference)) {

        const reflength = reference.length;

        for (const array of items) {

            // Toutes les valeurs sont des tableaux
            if (!Array.isArray(array)) {
                log(`All elements are not arrays:`, reference, array);
                return false;
            }

            // Tous les tableux contiennent le même nombre d'elements
            if (reflength !== array.length) {
                log(`All arrays do not contains the same number of elements.`, reference, array);
                return false;
            }

            // Chaque valeur de la ref doit être présente dans les autres items
            searchForExistance:
            for (const refval of reference) {

                // La valeur de référence existe dans l'un des items de l'array
                for (let i = 0; i < array.length; i++)
                    if (jsonDeepEquals([refval, array[i]], debug, [...path, i]))
                        // Valeur trouvée dans l'un des item, passage à la valeur suivante
                        continue searchForExistance;

                // La valeur n'a pas été trouvée
                log(`Value ${JSON.stringify(refval)} has not been found in all arrays.`);
                return false;

            }
        }

    // Objet = itération élements
    } else if (typeof reference === "object") {

        // Toutes les valeurs sont des objets
        if (items.every((i) => typeof i !== "object")) {
            log(`All elements are not objects.`);
            return false;
        }

        // Les objets sont les mêmes
        for (const key in reference) {
            const vref = reference[key];
            const vres = items.map((r) => r[key]);

            if (!jsonDeepEquals([vref, ...vres], debug, [...path, key])) 
                return false;
        }

    // Sinon, égalités srictes
    } else {

        // Toutes les valeurs sont les mêmes
        if (items.some((r) => r !== reference)) {
            log(`All elements are not equal:`, reference, ...items);
            return false;
        }
    }

    return true;
}