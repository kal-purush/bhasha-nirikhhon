// Angular imports
import { NgModule, ModuleWithProviders, APP_INITIALIZER, ErrorHandler } from '@angular/core';
import { CommonModule }                                                 from '@angular/common';
import { FormsModule }                                                  from '@angular/forms';
import { HttpModule }                                                   from '@angular/http';

// Core imports
import { LoggingService }       from './services/logging-service/logging.service';
import { LoggingErrorHandler }  from './services/logging-service/logging-error-handler';
import { AppConfig }            from './models';
import { ConfigurationService } from './services/configuration-service';

import { provideHotStore }			from './bootstrap/hmr';
import reducers									from './store/reducers';
import actions									from './store/actions';

// Core exports
export { AppState }             from './store/reducers/index';
export { ConfigurationService } from './services/configuration-service';
export { CounterActions	}       from './store/actions';

export function provideCore(configPath: string): Array<any> {
	return [
		AppConfig,
		ConfigurationService,
		LoggingService, {
			provide: ErrorHandler,
			useClass: LoggingErrorHandler
		}, {
			provide: APP_INITIALIZER,
			useFactory: (configurationService: ConfigurationService) => () => {
				return configurationService.loadConfiguration(configPath);
			},
			deps: [ConfigurationService, HttpModule],
			multi: true
		}
	];
}

@NgModule({})
export class CoreModule {
	static provideCore(configPath: string): ModuleWithProviders {
		return {
			ngModule: CoreModule,
			providers: provideCore(configPath)
		};
	}
}
