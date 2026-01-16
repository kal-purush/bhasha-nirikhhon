import { Controller } from '../Controller';

export class Error200Controller extends Controller {

    OK(body: any) {
        this.status(200).send(body);
    }

    Created(body: any) {
        this.status(201).send(body);
    }

    Accepted(body: any) {
        this.status(202).send(body);
    }

    NonAuthoritativeInformation(body: any) {
        this.status(203).send(body);
    }

    NoContent(body: any) {
        this.status(204).send(body);
    }

    ResetContent(body: any) {
        this.status(205).send(body);
    }

    PartialContent(body: any) {
        this.status(206).send(body);
    }

    MultiStatus(body: any) {
        this.status(207).send(body);
    }
    AlreadyReported(body: any) {
        this.status(208).send(body);
    }
    IMUsed(body: any) {
        this.status(226).send(body);
    }


}