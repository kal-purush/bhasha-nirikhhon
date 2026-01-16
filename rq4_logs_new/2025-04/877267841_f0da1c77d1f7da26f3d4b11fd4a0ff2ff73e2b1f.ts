export const userFilter = (query: { type: string, name: string, group: string, location: string, availability: string }) => {
    const filter: any = {};
    if (query.type) filter.role = query.type;
    if (query.name) filter.name = { $regex: query.name, $options: "i" };
    if (query.group) filter["serviceProviderDetails.group"] = query.group;
    if (query.location) filter.location = query.location;
    if (query.availability) filter["serviceProviderDetails.availability"] = query.availability;
    return filter;
}