const { RESTDataSource } = require('apollo-datasource-rest')

class PlantAPI extends RESTDataSource {
    constructor() {
        super();
        this.baseURL = 'https://trefle.io/api/';
    }

    willSendRequest(request) {
        request.headers.set('Authorization', this.context.token);
    }

    async getPlants(id) {
        const response = await this.get(`plants/?q=${id}`)
        return Array.isArray(response)
            ? response.map(plant => this.plantReducer(plant))
            : [];
    }

    plantReducer(plant) {
        // console.log(plant)
        return {
            id: plant.id || 0,
            common_name: plant.common_name || 'unknown',
            complete_data: plant.complete_data,
            link: plant.link,
            scientific_name: plant.scientific_name,
            slug: plant.slug
        };
    }

    // async getLaunchById({ launchId }) {
    //     const response = await this.get('launches', { flight_number: launchId });
    //     return this.launchReducer(response[0]);
    // }
    // getLaunchesByIds({ launchIds }) {
    //     return Promise.all(
    //         launchIds.map(launchId => this.getLaunchById({ launchId }))
    //     )
    // }
}

module.exports = PlantAPI