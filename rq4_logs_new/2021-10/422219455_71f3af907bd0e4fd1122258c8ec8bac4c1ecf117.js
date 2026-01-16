const clientService = require('./service');

class AreaService {
  getAll(params) {
    return clientService.get('/api/v1/regions', {params});
  }
}

const areaService = new AreaService();
module.exports = areaService;