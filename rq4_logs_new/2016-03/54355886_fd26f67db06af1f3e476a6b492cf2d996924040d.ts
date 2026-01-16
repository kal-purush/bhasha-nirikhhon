/// <reference path="./kn-appcache.d.ts" />

export class AppCache implements KN.IAppCache {
    private _appStarted: number; // in milliseconds

    constructor(private $rootScope: ng.IRootScopeService,
                private $window: ng.IWindowService,
                private updateInterval: number) {
        'ngInject';
    }

    public init() {
        const appCache = this.$window.applicationCache;
        this._appStarted = Date.now();
        if ( appCache && appCache.status !== appCache.UNCACHED ) {
            this._startChecking();
            this._addListeners();
        }
    }

    private _getUptime() {
        return Date.now() - this._appStarted;
    }

    private _addListeners() {
        const appCache = this.$window.applicationCache;
        const action = () => this.$window.location.reload();

        appCache.addEventListener('updateready', () => {
            let uptime = this._getUptime();
            // Если страничка была открыта недавно (менее 30 секунд назад), то перезагрузить принудительно
            if ( uptime < 30 * 1000 ) {
                action();
                return;
            }

            // Если пользователь откроет другую страничку, обновится автоматически
            this.$rootScope.$on('$stateChangeSuccess', action);
        }, false);
    }

    private _startChecking() {
        setInterval(() => {
            this.$window.applicationCache.update();
        }, this.updateInterval);
    }
}