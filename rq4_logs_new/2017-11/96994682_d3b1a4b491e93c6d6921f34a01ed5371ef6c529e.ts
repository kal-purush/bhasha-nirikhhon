import { HapinessModule, Lifecycle, OnEvent, Request, ReplyWithContinue } from '@hapiness/core';
import { Biim } from './';

@Lifecycle({
    event: 'onPreResponse'
})
class ReformatBadRequestLifecycle implements OnEvent {

    onEvent(request: Request, reply: ReplyWithContinue) {
        if (request.response.isBoom && !request.response.output.payload['key']) {
            const source = request.response.output;
            const err = Biim.create(source.statusCode, source.payload.error);
            request.response.output.payload['key'] = err.output.payload['key'];
        }
        reply.continue();
    }

}

@HapinessModule({
    version: '1.0.0',
    declarations: [ ReformatBadRequestLifecycle ],
})
export class BiimModule {}