import {getPreprocessorFactory} from 'publisher';
import {PreprocessorConstructor, Preprocessor} from 'preprocessor';
import {File} from 'util/file';

let preprocessCalled: boolean;
let preprocessArgs: any[];

beforeEach(() => {
	preprocessCalled = false;
	preprocessArgs = [];
});

class TestPreprocessor implements Preprocessor {
	processFile(content: string, file: File, done: Function) {
		preprocessCalled = true;
		preprocessArgs = [content, file, done];
	}
}

describe('getPreprocessorFactory', () => {
	let preprocessorFactory: any;

	beforeEach(() => {
		preprocessorFactory = getPreprocessorFactory(TestPreprocessor);
	});

	it('should be a factory in the correct format', () => {
		expect(preprocessorFactory.length).toBe(2);
		expect(preprocessorFactory[0]).toEqual('factory');
		expect(preprocessorFactory[1] instanceof Function).toBe(true);
	});

	describe('the preprocess function returned by the factory fn', () => {
		let preprocess;

		beforeEach(() => {
			preprocess = preprocessorFactory[1]();
		});

		it('should call through to TestPreprocessor#processFile', () => {
			let content = 'content';
			let file = 'file';
			let done = () => { };

			preprocess(content, file, done);

			expect(preprocessCalled).toBe(true);
			expect(preprocessArgs).toEqual([content, file, done]);
		});
	});
});