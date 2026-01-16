import * as Inputs from './namespaces/Inputs';

export const parseInputs = (getInput: Inputs.GetInput): Inputs.Args => {
  const branch = getInput('branch');

  let notifications;
  const notify_check = getInput('notify_check');
  const notify_issue = getInput('notify_issue');
  const onlyNotify = getInput('only_notify') === 'true';

  if (notify_check || notify_issue) {
    const label = getInput('title');
    const token = getInput('token', { required: true });
    notifications = {
      token,
      label,
      check: notify_check === 'true',
      issue: notify_issue === 'true',
    };
  }

  return {
    branch,
    onlyNotify,

    notifications,
  };
};