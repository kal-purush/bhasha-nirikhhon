import {
	AbstractFunction,
	Event,
	EventResult,
	FunctionExecutionParameters,
	FunctionOutput,
	FunctionSignature,
	Parameter,
	Schema,
} from '@fincity/kirun-js';
import { NAMESPACE_UI_ENGINE } from '../constants';

const SIGNATURE = new FunctionSignature('DecodeURIComponent')
	.setNamespace(NAMESPACE_UI_ENGINE)
	.setParameters(new Map([Parameter.ofEntry('uriComponent', Schema.ofString('uriComponent'))]))
	.setEvents(
		new Map([
			Event.eventMapEntry(
				Event.OUTPUT,
				new Map([['decodedValue', Schema.ofString('decodedValue')]]),
			),
		]),
	);

export class DecodeURIComponent extends AbstractFunction {
	protected async internalExecute(context: FunctionExecutionParameters): Promise<FunctionOutput> {
		const uriComponent: string = context.getArguments()?.get('uriComponent');
		return new FunctionOutput([
			EventResult.outputOf(new Map([['decodedValue', decodeURIComponent(uriComponent)]])),
		]);
	}

	getSignature(): FunctionSignature {
		return SIGNATURE;
	}
}