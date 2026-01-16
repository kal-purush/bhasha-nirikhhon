import * as is from 'is';
import * as logging from '@google-cloud/logging';
import * as winston from 'winston';
import * as os from 'os';

/**
 * Map of npm output levels to Stackdriver Logging levels.
 *
 * @type {object}
 * @private
 */
const NPM_LEVEL_NAME_TO_CODE = {
	error: 3,
	warn: 4,
	info: 6,
	verbose: 7,
	debug: 7,
	silly: 7
};

const LEVEL_NAME_TO_STACKDRIVER_CODE = {
	fatal: 2,
	error: 3,
	warn: 4,
	success: 5,
	info: 6,
	debug: 7,
	trace: 7
};

/**
 * Map of Stackdriver Logging levels.
 *
 * @type {object}
 * @private
 */
const STACKDRIVER_LOGGING_LEVEL_CODE_TO_NAME = {
	0: 'emergency',
	1: 'alert',
	2: 'critical',
	3: 'error',
	4: 'warning',
	5: 'notice',
	6: 'info',
	7: 'debug'
};

/**
 * This module provides support for streaming your winston logs to
 * [Stackdriver Logging]{@link https://cloud.google.com/logging}.
 *
 * If your app is running on Google Cloud Platform, all configuration and
 * authentication is handled for you. We also auto-detect the appropriate
 * resource descriptor to report the log entries against.
 *
 * If you are running your application in another environment, such as locally,
 * on-premise, or on another cloud provider, you will need to provide additional
 * configuration.
 *
 * @constructor
 * @alias module:logging-winston
 *
 * @param {object} options - [Configuration object](#/docs). Refer to this link
 *     for authentication information.
 * @param {object=} options.level - The default log level. Winston will filter
 *     messages with a severity lower than this.
 * @param {object=} options.levels - Custom logging levels as supported by
 *     winston. This list is used to translate your log level to the Stackdriver
 *     Logging level. Each property should have an integer value between 0 (most
 *     severe) and 7 (least severe). If you are passing a list of levels to your
 *     winston logger, you should provide the same list here.
 * @param {string=} options.logName - The name of the log that will receive
 *     messages written to this transport. Default: `winston_log`
 * @param {object=} options.resource - The monitored resource that the transport
 *     corresponds to. On Google Cloud Platform, this is detected automatically,
 *     but you may optionally specify a specific monitored resource. For more
 *     information see the
 *     [official documentation]{@link https://cloud.google.com/logging/docs/api/reference/rest/v2/MonitoredResource}.
 *
 * @example
 *
 * winston.add(transport, {
 *   projectId: 'grape-spaceship-123',
 *   keyFilename: '/path/to/keyfile.json',
 *   level: 'warning', // log at 'warning' and above
 *   resource: {
 *     type: 'global'
 *   }
 * });
 *
 * winston.emerg('antimatter containment field collapse imminent');
 */
class WinstonStackdriver extends winston.Transport {

	private levels;
	private logger;
	private labels;
	private resource;
	private hostname: string;

	constructor(options) {
		super(options);
		options = {
			scopes: ['https://www.googleapis.com/auth/logging.write'],
			...options
		};

		// Levels
		this.levels = options.levels || LEVEL_NAME_TO_STACKDRIVER_CODE || NPM_LEVEL_NAME_TO_CODE;

		// Resource
		if (options.projectId) {
			options.resource = options.resource || {};
			options.resource.type = options.resource.type || 'global';
			options.resource.labels = options.resource.labels || {};
			options.resource.labels.project_id = options.resource.labels.project_id || options.projectId;
		}
		this.resource = options.resource;

		// Labels
		if (options.storeHost) this.hostname = os.hostname();
		let labels = options.labels || {};
		if (options.storeHost) {
			labels.hostname = this.hostname;
		}
		if (options.label) {
			labels.label = options.label;
		}
		this.labels = labels;

		// Actually initialize logger
		let logName = options.logName || 'winston_log';
		this.logger = logging(options).log(logName);
	}

	log(levelName, msg, metadata, callback) {
		if (is.fn(metadata)) {
			callback = metadata;
			metadata = {};
		}

		if (!this.levels[levelName]) {
			throw new Error('Unknown log level: ' + levelName);
		}

		let levelCode = this.levels[levelName];
		let stackdriverLevel = STACKDRIVER_LOGGING_LEVEL_CODE_TO_NAME[levelCode];

		let labels = this.labels || {};

		//   if (is.object(metadata)) {
		//     // We attach properties as labels on the log entry. Logging proto requires
		//     // that the label values be strings, so we convert using util.inspect.
		//     for (var key in metadata) {
		// 		if (typeof metadata[key] === 'string') {
		// 			labels[key] = metadata[key];
		// 		} else {
		// 			labels[key] = util.inspect(metadata[key]);
		// 		}
		//     }
		//   }

		labels.module = metadata.module;

		let entryMetadata = {
			resource: this.resource,
			labels: labels,
		};

		// Stackdriver Logs Viewer picks up the summary line from the `message`
		// property of the jsonPayload.
		// https://cloud.google.com/logging/docs/view/logs_viewer_v2#expanding.
		//
		// For error messages at severity 'error' and higher, Stackdriver
		// Error Reporting will pick up error messages if the full stack trace is
		// included in the textPayload or the message property of the jsonPayload.
		// https://cloud.google.com/error-reporting/docs/formatting-error-messages
		// We prefer to format messages as jsonPayload (by putting it as a message
		// property on an object) as that works is accepted by Error Reporting in
		// for more resource types.
		//
		// TODO(ofrobots): when resource.type is 'global' we need to additionally
		// provide serviceContext.service as part of the entry for Error Reporting to
		// automatically pick up the error.
		if (metadata && metadata.stack) {
			msg += (msg ? ' ' : '') + metadata.stack;
		}

		let params = metadata.params || {};
		let data = {
			message: msg,
			...params,
		};
		let entry = this.logger.entry(entryMetadata, data);
		this.logger[stackdriverLevel](entry, callback);
	}

}

(<any>winston.transports).Stackdriver = WinstonStackdriver;


export default WinstonStackdriver;