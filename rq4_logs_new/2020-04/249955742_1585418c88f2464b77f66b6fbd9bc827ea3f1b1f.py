from aiounittest import AsyncTestCase
from discord.ext.commands import Cog
from ..components import InfoComponentsCommands
from .fakers import BOT
from .results import InfoComponentsCommandsTestResult


class InfoComponentsCommandsTest(AsyncTestCase):
    """ Async Test case for cog InfoComponentsCommands """

    def setUp(self):
        """ Init tests with cog and expected results """
        self.cog = InfoComponentsCommands(BOT)
        self.result = InfoComponentsCommandsTestResult()
        self.maxDiff = None

    def test_init(self):
        """ assert after init is instance Cog, the number of commands
        and if they have good name and help """
        self.assertIsInstance(self.cog, Cog)
        commands = self.cog.get_commands()
        self.assertEqual(len(commands), 1)
        c_tuples = [(c.name, c.help) for c in commands]
        for i in range(len(commands)):
            self.assertTupleEqual(c_tuples[i], self.result.init_method[i])