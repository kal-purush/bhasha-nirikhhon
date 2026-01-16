import { Command} from './command';
export const COMMANDS: Command[] = [
  {
    Guid: 'command1', Name: 'FakeCommand', Entity: 'Entity', EntityGuid: 'entity1',
    CreatedOn: '2017-01-01T00:00:00', DisplayProperties: { title: 'fake title', description: 'fake description' }
  }
];

export class CommandsServiceStub {
  commands: Array<Command> = new Array<Command>();
  constructor() {
    COMMANDS.forEach(element => {
      this.commands.push(element);
    });
  }
  getCommands(searchText: string): Promise<Array<Command>> {
    return Promise.resolve(this.commands);
  };
  getCommand(guid: string): Promise<Command> {
    return Promise.resolve(this.commands.find(f => f.Guid === guid));
  };
};