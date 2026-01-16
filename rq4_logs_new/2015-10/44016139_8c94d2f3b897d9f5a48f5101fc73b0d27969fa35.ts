import IAddressFinder = require("./IAddressFinder");
import Address = require("../data/common/Address");
import CorreiosMiddleware = require("./correiosMiddleware/CorreiosMiddleware");

class CorreiosAdapter implements IAddressFinder {

	public constructor() { }

	public findAddress(zip: string, callback: (address: Address) => void): void {
		var json = CorreiosMiddleware.findAddress(zip, function(data: string) {
			var rawAddress = JSON.parse(data);
			var address = new Address(
				rawAddress.logradouro,
				0,
				rawAddress.bairro,
				rawAddress.localidade,
				rawAddress.uf,
				rawAddress.cep);
			callback(address);
		});
	}
}

export = CorreiosAdapter;