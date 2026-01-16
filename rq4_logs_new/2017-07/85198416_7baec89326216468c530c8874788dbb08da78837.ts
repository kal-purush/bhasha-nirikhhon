import { FetchTestConfig, FetchHelperConfig} from "../../shared/fetch/helpers";
import { expect } from "chai";
import {expectNoActions, getActions} from "../../actionHelpers";
import { promiseJSON} from "../../mocks/mockRemote";
import {Sandbox} from "../../Sandbox";
import {ErrorInfo, Result, ResultStatus} from "../../../main/shared/models/Generated";
import { Notification } from "../../../main/shared/actions/NotificationActions";
import {ReportingFetcher} from "../../../main/report/sources/ReportingFetcher";
import fetcher, { FetchOptions } from "../../../main/shared/sources/Fetcher";

function mockFetcherResponse<T>(data?: Result, errorMessage?: string) {
    data = data || {
            status: "success",
            data: {},
            errors: []
        };

    const promise = new Promise<Response>(function (resolve, reject) {
        if (errorMessage) {
            reject(errorMessage);
        } else {
            resolve(promiseJSON(data));
        }
    });

    mockFetcher(promise);
}

function mockResult<T>(data: T,
                              errors?: Array<ErrorInfo>,
                              status?: ResultStatus): Result {
    errors = errors || [];
    status = status || "success";

    return { data, errors, status };
}

function mockFetcher(promise: Promise<Response>) {
    fetcher.fetcher = new ReportingFetcher();
    fetcher.fetcher.fetchFromReportingApi = function(urlFragment: string, options?: FetchOptions, includeToken: boolean = true) {
        return promise;
    };
}

export class ReportingFetchHelper<TPayload> {
    config: FetchHelperConfig<TPayload>;
    sandbox: Sandbox;

    constructor(config: FetchHelperConfig<TPayload>) {
        this.config = config;
    }

    testFetchWithMockedResponse({ done, payload, errorMessage, expectedAction }: FetchTestConfig) {
        mockFetcherResponse(payload, errorMessage);

        const fetcherSpy = this.sandbox.sinon.spy(fetcher.fetcher, "fetchFromReportingApi");
        const dispatchSpy = this.sandbox.dispatchSpy();
        const handler = () => {
            try {
                expect(fetcherSpy.args[0][0]).to.equal(this.config.expectedURL);
                const actions = getActions(dispatchSpy);
                expect(actions[0].action).to.contain("beginFetch");
                expect(actions.length).to.be.greaterThan(1, `Expected '${expectedAction.action}' to be triggered`);
                expect(actions[1].action).to.contain(expectedAction.action);
                expect(actions[1].payload).to.eql(expectedAction.payload);
                done();
            } catch (e) {
                done(e);
            }
        };
        this.config.triggerFetch().then(handler, handler);
    }

    addTestsToMocha() {
        beforeEach(() => this.sandbox = new Sandbox());
        afterEach(() => this.sandbox.restore());

        it(`emits update when source returns successfully`, (done: DoneCallback) => {
            const payload = this.config.makePayload();
            this.config.prepareForFetch();
            this.testFetchWithMockedResponse({
                done,
                payload: mockResult(payload),
                errorMessage: null,
                expectedAction: {
                    action: "update",
                    payload: payload
                }
            });
        });

        it("emits notificationActions.notify when source returns errors", (done: DoneCallback) => {
            const message = "Error message in error collection";
            const errors: Array<ErrorInfo> = [
                { code: "code", message: message }
            ];
            const expectedNotification: Notification = {
                message: message,
                type: "error"
            };
            this.config.prepareForFetch();
            this.testFetchWithMockedResponse({
                done,
                payload: mockResult(null, errors, "failure"),
                errorMessage: null,
                expectedAction: {
                    action: "NotificationActions.notify",
                    payload: expectedNotification
                }
            });
        });

        it("emits notificationActions.notify when error occurs accessing source", (done: DoneCallback) => {
            const errorMessage = "Error message";
            const expectedNotification: Notification = {
                message: errorMessage,
                type: "error"
            };
            this.config.prepareForFetch();
            this.testFetchWithMockedResponse({
                done,
                payload: null,
                errorMessage,
                expectedAction: {
                    action: "NotificationActions.notify",
                    payload: expectedNotification
                }
            });
        });

        it("triggers logout when server returns bearer token error", (done: DoneCallback) => {
            const errors: Array<ErrorInfo> = [
                {
                    code: "bearer-token-invalid",
                    message: "Something wicked this way comes"
                }
            ];
            this.config.prepareForFetch();
            this.testFetchWithMockedResponse({
                done,
                payload: mockResult(null, errors, "failure"),
                errorMessage: null,
                expectedAction: {
                    action: "AuthActions.logOut",
                    payload: true
                }
            });
        });

        if (this.config.prepareForCachedFetch) {
            it("does not fetch when data is present in cache", (done: DoneCallback) => {
                mockFetcherResponse(mockResult(true));
                const fetcherSpy = this.sandbox.sinon.spy(fetcher.fetcher, "fetchFromReportingApi");

                this.config.prepareForCachedFetch();
                const dispatchSpy = this.sandbox.dispatchSpy();
                const handler = () => {
                    try {
                        expect(fetcherSpy.called).to.equal(false, "Cache miss - fetcher was invoked");
                        expectNoActions(dispatchSpy);
                        done();
                    } catch (e) {
                        done(e);
                    }
                };
                this.config.triggerFetch().then(handler, handler);
            });
        }
    }
}