require('cliff');
import { runCommand } from '../../util/command';
import { securityMappingOutputFormat } from './util';

export = {
	command: 'security-mappings [id]',
	desc: 'List transfer security mappings',
	handler: argv => {
		runCommand(argv, async (client, output) => {
			const mappings = await client.transferSecurityMappings.page({ id: argv.id });
			if (output.outputJson) {
				output.writeItem(mappings);
			} else {
				output.writeTable(mappings.items, securityMappingOutputFormat);
			}
		});
	}
};