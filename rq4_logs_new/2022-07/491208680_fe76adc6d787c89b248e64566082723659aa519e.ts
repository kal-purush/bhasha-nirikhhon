import { StatusCode } from "@constant/status-code.enum";
import { ApiProperty } from "@nestjs/swagger";
import { MessageResponse } from "./message-response";

export class HttpChartResponse<T> {
    @ApiProperty()
    statusCode: StatusCode;

    @ApiProperty()
    message: string;

    labels: Array<string>;

    data: T;

    static build<T>(): HttpChartResponseBuilder<T> {
        return new HttpChartResponseBuilder<T>();
    }

    static success<T>(data?: T, labels?: Array<string>): HttpChartResponse<T> {
        return this.build<T>()
            .withStatusCode(StatusCode.OK)
            .withMessage(MessageResponse[StatusCode.OK])
            .withLabels(labels)
            .withData(data)
            .build()

    }
}

export class HttpChartResponseBuilder<T>  {

    private statusCode: StatusCode;
    private message: string;
    private labels: Array<string>;
    private data: T;

    withStatusCode(statusCode: StatusCode): HttpChartResponseBuilder<T> {
        this.statusCode = statusCode;
        return this;
    }

    withMessage(message: string): HttpChartResponseBuilder<T> {
        this.message = message;
        return this;
    }

    withData(data: T): HttpChartResponseBuilder<T> {
        this.data = data;
        return this;
    }


    withLabels(labels: Array<string>): HttpChartResponseBuilder<T> {
        this.labels = labels;
        return this;
    }



    build(): HttpChartResponse<T> {
        const httpPagingResponse = new HttpChartResponse<T>();
        httpPagingResponse.statusCode = this.statusCode;
        httpPagingResponse.message = this.message;
        httpPagingResponse.labels = this.labels;
        httpPagingResponse.data = this.data;
        return httpPagingResponse;
    }
}