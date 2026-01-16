import {IAmADomainEvent} from "./iamadomainevent";

interface IView {
   name: string;

   handle(event: IAmADomainEvent);
}