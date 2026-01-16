import chai from 'chai';
const expect = chai.expect;

import DocTestingService from './services/DocTestingService.js';

const CONFIG = {
	COMPONENT_FOLDER_LIST: [],
	COMPONENT_CONFIG: {}
};

//

before(async () => {
	CONFIG.COMPONENT_FOLDER_LIST = await DocTestingService.getFolderContents(__dirname + '/../../src/components/');
});


describe('all components', () => {

	describe('allComponents', () => {
		it('at least 1 component should exist', (done) => {
			let componentNameList = getComponentNameList();

			expect(componentNameList.length).to.be.greaterThan(0);
			done();
		});

		describe('each component', () => {
			it('should all have a readme file', async (done) => {
				try {
					let readmes;
					readmes = await Promise.all(getComponentNameList().map((componentName) => {
						return new Promise(async (resolve, reject) => {
							let response = '';

							try {
								response = await DocTestingService.getReadmeFileContents(componentName);
							} catch (error) {

							}

							resolve({componentName, readme: response});
						});
					}));

					let defaultReadmeContents = '[DESCRIPTION]';
					let componentsWithoutAReadme = readmes.filter((readmeConfig) => readmeConfig.readme.includes(defaultReadmeContents)).map(({componentName}) => componentName);

					if (componentsWithoutAReadme.length > 0) {
						componentsWithoutAReadme.forEach((componentName) => {
							console.error(componentName + ' needs a README.md');
						})
					}

					expect(componentsWithoutAReadme).property('length').to.equal(0);
					done();
				} catch (error) {
					done(error);
				}
			});
		});
	});

});



function getComponentNameList() {
	return CONFIG.COMPONENT_FOLDER_LIST.filter((name) => name[0] !== '.');
}