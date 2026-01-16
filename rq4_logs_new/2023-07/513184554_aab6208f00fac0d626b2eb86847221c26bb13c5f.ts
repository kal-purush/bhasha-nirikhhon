
const setFromPath = (obj: any, path: any, value: any): void => {
    if (path.length === 0) return obj;
    const next = path.shift();
    if (path.length === 0) {
        obj[next] = value;
    } else {
        if (obj[next] == null) obj[next] = {};
        setFromPath(obj[next], path, value);
    }
};
const set = (obj: any, string: string, value: any): void => { setFromPath(obj, string.split('.'), value); };

type GSTCapParameter = {
    type: "atom",
    value: string,
} | {
    type: "range",
    max: string,
    maxdenom?: string,
    min: string,
    mindenom?: string,
} | {
    type: "choice",
    list: string[]
}

interface GSTCap {
    type: string,
    parameters: Record<string, GSTCapParameter>
}

interface GSTDevice {
    type?: string,
    name?: string[],
    class?: string[],
    properties?: {
        device?: {
            api: string,
            name: string,
            path: string
        }
    },
    caps?: GSTCap[],
    [other: string]: any,
}

/** parse the output from gst-device-monitor-1.0 and return as JSON object */
export default (output: string): GSTDevice[] => {

    const lines = output
        .split(/\r?\n/)
        .filter(x => x.length > 0)
        .map(line => line.replace(/\t/g, '    '));

    // annotate the lines with some syntactic, but still local information
    const annotatedLines = lines.map((line) => {
        const field = line.match(/^\s*(?<key>[a-z\s]*)\s*:(?<value>.*)/)?.groups;
        return {
            line,
            indentation: line.search(/\S/),
            field,
            property: (field == null) && line.match(/^\s*(?<key>[a-z0-9-_\.]*) = (?<value>.*)/)?.groups,
            type: line.match(/^\s*gst-launch-1.0 (?<type>\S*) .*!.*$/)?.groups?.type
        };
    });

    // now iterate through the lines and process based on context
    const devices: GSTDevice[] = [];
    let device: GSTDevice = {};
    let field;
    let properties = false;

    for (const i of annotatedLines.map((_, i) => i)) {
        const current = annotatedLines[i];
        if (current.line === 'Device found:') {
            // start new device object
            device = {};
            devices.push(device);
            properties = false;
            field = null;
        } else if (current.type != null) {
            device.type = current.type;
        } else if (current.field != null) {
            // start new field
            field = current.field.key.trim();
            if (field === 'properties') {
                properties = true;
                device[field] = {};
            } else {
                properties = false;
                device[field] = [];
                const trimmed = current.field.value.trim();
                trimmed.length > 0 && device[field].push(trimmed);
            }
        } else if (field != null) {
            // continuation of previous field
            if (properties) {
                if (current.property != null) {
                    set(device[field], current.property.key, current.property.value);
                }
            } else {
                const trimmed = current.line.trim();
                trimmed.length > 0 && device[field].push(trimmed);
            }
        }
    }

    // some post-processing of values
    devices.forEach(device => {
        device.caps = device.caps
            ?.map((cap: any) => {
                const groups = cap.match(/^(?<type>[^,]*), (?<parameters>.*)/)?.groups;
                if (groups != null) {
                    const parameters = parseCapParameters(groups.parameters);
                    const cap: GSTCap = {
                        type: groups.type,
                        parameters
                    };
                    return cap;
                } else
                    return undefined;
            }).filter(x => x) as GSTCap[];
    });

    return devices;
};


/** parse things link
 format=(string)YUY2, \
 width=(int)1280, \
 height=(int)960, \
 pixel-aspect-ratio=(fraction)1/1, \
 framerate=(fraction){ 15/2, 5/1 };
 
 or (ubuntu22):
 format=YUY2, \
 width=640, \
 height=480, \
 framerate={ (fraction)30/1, (fraction)25/1 };
 */
const parseCapParameters = (params: string | undefined): Record<string, GSTCapParameter> => {
    if (params == null) return {};

    const groups = params
        .match(/(?<name>[a-z\-]*)=(\((?<type>[a-z]*)\))?(?<rest>.*)/)?.groups;
    if (groups == null) return {};

    const values: Record<string, any> = {};
    groups.type && (values.valueType = groups.type);
    const rtv = {
        [groups.name]: values
    };

    let nextRest;
    // match from rest the value part
    if (groups.rest[0] === '{') {
        // it's a choice
        const valueMatch = groups.rest.match(/\{(?<list>[^\}]*)\}(?<rest>.*)/)?.groups;
        if (valueMatch == null) {
            console.warn('parse error: choice', groups.rest);
        } else {
            values.type = 'choice';
            values.list = valueMatch.list.split(', ').map(s =>
                s.trim().replace(/^\((?<type>[a-z]*)\)/, ''));
            nextRest = valueMatch.rest;
        }

    } else if (groups.rest[0] === '[') {
        // it's a range
        const valueMatch = groups.rest
            .match(/\[ (?<min>\d*)(\/(?<mindenom>\d*))?, (?<max>\d*)(\/(?<maxdenom>\d*))?(, (?<step>\d*))? \](?<rest>.*)/)?.groups;
        if (valueMatch == null) {
            console.warn('parse error: range', groups.rest);
        } else {
            values.type = 'range';
            values.min = valueMatch.min;
            valueMatch.mindenom && (values.mindenom = valueMatch.mindenom);
            values.max = valueMatch.max;
            valueMatch.maxdenom && (values.maxdenom = valueMatch.maxdenom);
            valueMatch.step && (values.step = valueMatch.step);
            nextRest = valueMatch.rest;
        }

    } else {
        // regular value
        const valueMatch = groups.rest.match(/(?<value>[^, ;]*)(?<rest>.*)/)?.groups;
        if (valueMatch == null) {
            console.warn('parse error: atom', groups.rest);
        } else {
            values.type = 'atom';
            values.value = valueMatch.value;
            nextRest = valueMatch.rest;
        }
    }

    const sub = parseCapParameters(nextRest);
    Object.assign(rtv, sub);

    return rtv as Record<string, GSTCapParameter>;
};