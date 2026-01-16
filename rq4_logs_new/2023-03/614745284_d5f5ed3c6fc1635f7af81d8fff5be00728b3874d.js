/**
 * To avoid the duplicating the extension component sections
 * We will use this function to check if it is registered with the same the positionId
 *
 * @param positionId
 * @param locationId
 * @returns {Promise<boolean>}
 */
export const isRegisteredSection = async (positionId, locationId) => {
    // Get extensionComponentSections list
    const registeredSections = await Shopware.State.get('extensionComponentSections').identifier[positionId] ?? [];

    let isRegister = false;
    registeredSections.forEach((section) => {
        if (section.props.locationId === locationId) {
            isRegister =  true;
        }
    });

    return isRegister;
}