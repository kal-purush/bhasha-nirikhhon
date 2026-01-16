import * as express from 'express';
import * as q from 'q';

export class ExpressServer {

    // Attributes
        server: express.Express; 

    // Methods
        constructor () {
            this.server = express();
        }

        public use (middle: any) {
            this.server.use(middle);
        }

        public setupStatics (route: string, staticsPath: any) {
            this.server.use(route, express.static(staticsPath));
        }

        public listen (port: number) {
            var deferred = q.defer();
            this.server.listen(port, deferred.resolve);
            return deferred.promise;
        }


}