/* User Story: Frappe 68 */

import { CommandHandler } from '@tshio/command-bus';
import { DeleteUserCommand } from './DeleteUserCommand';
import { UserDeleter } from './UserDeleter';

type Props = {
  readonly userDeleter: UserDeleter;
};

export class DeleteUserCommandHandler implements CommandHandler<DeleteUserCommand> {
  private readonly userDeleter: UserDeleter;

  readonly commandType = DeleteUserCommand.name;

  constructor({ userDeleter }: Props) {
    this.userDeleter = userDeleter;
  }

  async execute(command: DeleteUserCommand) {
    const { id } = command.payload;

    return this.userDeleter.execute(id);
  }
}