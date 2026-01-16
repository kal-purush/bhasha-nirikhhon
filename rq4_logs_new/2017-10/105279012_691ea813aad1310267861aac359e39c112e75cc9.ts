import LambdaContext from '../lambda/lambda-context';
import Segment from '../segment';

export default class LambdaSegment extends Segment {

	constructor () {

		super('facade');

		const lambda = LambdaContext.segment;
		this._id = lambda.id;
		this._traceId = lambda.trace_id;
		this._startTime = lambda.start_time;
		this._isSampled = false;

	}

	public async flush (): Promise<void> {

		await this._flushSubSegments(true);

	}

	public async close () {

		this._closed = true;

		await this.flush();

	}

}