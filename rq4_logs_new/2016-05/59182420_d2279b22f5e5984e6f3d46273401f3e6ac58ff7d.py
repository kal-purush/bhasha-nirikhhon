from commands.command import _BaseCommand
from listeners import OnClientDisconnect
from messages import SayText2


# We don't translate this message to avoid extra
# language/translation lookup operations
ANTI_SPAM_TEXT = "You're spamming the command"


anti_spam_message = SayText2(message=ANTI_SPAM_TEXT)

# Storage for all instances of inherited from BaseCommand classes
_spam_proof_commands = []


class BaseSpamProofCommand(_BaseCommand):
    def __init__(self, timeout, names, *args, **kwargs):
        super().__init__(names, *args, **kwargs)

        self.timeout = timeout

        # This is where time() results for each client will be stored at
        self.client_timestamps = {}

        # Put ourselves in global storage
        _spam_proof_commands.append(self)

    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def _unload_instance(self):
        self._manager_class.unregister_commands(self.names, self.callback)

        _spam_proof_commands.remove(self)


@OnClientDisconnect
def listener_on_client_disconnect(index):
    for spam_proof_command in _spam_proof_commands:
        if index in spam_proof_command.client_timestamps:
            del spam_proof_command.client_timestamps[index]