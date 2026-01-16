import {Injectable} from "@angular/core";
import {AuthHttp} from "../angular2-jwt";
import {BACKEND_URL} from "../backend-config/backend-config";
import {DoseSeries} from "../dose-series/dose-series";
import {RequestOptions, Headers} from "@angular/http";

/**
 * Service for fetching and saving DoseEvent instances.
 */
@Injectable()
export class DoseSeriesService {

    private doseSeriesUrl: string = `${BACKEND_URL}/dose-series`;

    constructor(
        private http: AuthHttp
    ) {
    }

    /**
     * Get a list of DoseSeries.
     * @returns {Promise<Array<DoseSeries>>}
     */
    public list(): Promise<Array<DoseSeries>> {
        return this.http.get(
            this.doseSeriesUrl
        ).toPromise().then(
            (response) => response.json()
        ).catch(
            this.handleError
        );
    }

    /**
     * Retrieves an existing DoseSeries from the server.
     * @param id to lookup.
     * @returns {Promise<DoseSeries>}.
     */
    public get(
        id: number
    ): Promise<DoseSeries> {
        return this.http.get(
            `${this.doseSeriesUrl}/${id}`
        ).toPromise().then(
            (response) => response.json()
        ).catch(
            this.handleError
        );
    }

    /**
     * Creates a new DoseSeries object.
     * @param doseSeries to save.
     * @returns {Promise<DoseSeries>}.
     */
    private post(
        doseSeries: DoseSeries
    ): Promise<DoseSeries> {

        const headers = new Headers(
            {
                "Content-Type": "application/json"
            }
        );

        const options = new RequestOptions(
            {
                headers: headers
            }
        );

        return this.http.post(
            this.doseSeriesUrl,
            JSON.stringify(doseSeries),
            options
        ).toPromise().then(
            res => res.json()
        ).catch(
            this.handleError
        );
    }

    /**
     * Updates an existing DoseSeries object.
     * @param doseSeries to save.
     * @returns {Promise<DoseSeries>}.
     */
    private put(
        doseSeries: DoseSeries
    ): Promise<DoseSeries> {

        const headers = new Headers(
            {
                "Content-Type": "application/json"
            }
        );

        const options = new RequestOptions(
            {
                headers: headers
            }
        );

        return this.http.put(
            `${this.doseSeriesUrl}/${doseSeries.id}`,
            JSON.stringify(doseSeries),
            options
        ).toPromise().then(
            res => res.json()
        ).catch(
            this.handleError
        );
    }

    /**
     * Saves an existing or creates a new Dose Series.
     * @param doseSeries to save.
     * @returns {Promise<DoseSeries>}.
     */
    public save(
        doseSeries: DoseSeries
    ): Promise<DoseSeries> {

        if (doseSeries.id) {
            return this.put(doseSeries);

        } else {
            return this.post(doseSeries);
        }

    }

    /**
     * Handles any errors communicating with backend.
     * @param error
     * @returns {Promise<void>|Promise<T>}
     */
    private handleError(error: any) {
        console.error("An error occurred", error);
        return Promise.reject(error.message || error);
    }

}