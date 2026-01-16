import { Command, CommandParameters } from '../../commands/command';
import { Contact } from '../contact';
export class ContactCommand extends Command {
    constructor(name: string, contact: Contact) {
        super(name, 'Contact', contact.guid);
    };
}

export class CreateContactParameters extends CommandParameters {
    Name: string;
}
export class CreateContactCommand extends ContactCommand {
    constructor(contact: Contact) {
        super('Create', contact);
        const parameters = new CreateContactParameters();
        parameters.Name = contact.name;
        this.Parameters = parameters;
    }
}

export class DeleteContactCommand extends ContactCommand {
    constructor(contact: Contact) {
        super('Delete', contact);
    }
}


export class RenameContactParameters extends CommandParameters {
    Name: string;
    OriginalName: string;
}
export class RenameContactCommand extends ContactCommand {
    constructor(contact: Contact, orgName: string) {
        super('Rename', contact);
        const parameters = new RenameContactParameters();
        parameters.OriginalName = orgName;
        parameters.Name = contact.name;
        this.Parameters = parameters;
    }
}

export class ChangeEmailForContactParameters extends CommandParameters {
    Email: string;
    OriginalEmail: string;
}
export class ChangeEmailForContactCommand extends ContactCommand {
    constructor(contact: Contact, orgEmail: string) {
        super('ChangeEmailFor', contact);
        const parameters = new ChangeEmailForContactParameters();
        parameters.OriginalEmail = orgEmail;
        parameters.Email = contact.email;
        this.Parameters = parameters;
    }
}

export class ChangeBirthDateParameters extends CommandParameters {
    birthDate?: Date;
    originalBirthDate?: Date;
}
export class ChangeBirthDateOfContactCommand extends ContactCommand {

    constructor(contact: Contact, orgBirthDate?: Date) {
        super('ChangeBirthDateOf', contact);
        const parameters = new ChangeBirthDateParameters();
        parameters.originalBirthDate = orgBirthDate;
        parameters.birthDate = contact.birthDate;
        this.Parameters = parameters;
    }
}