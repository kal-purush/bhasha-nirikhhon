import os

from nose.plugins import Plugin


class MpSplitPlugin(Plugin):
    name = 'mp-split'

    def options(self, parser, env=os.environ):
        super(MpSplitPlugin, self).options(parser, env)

        parser.add_option(
            '--mp-split-all',
            action='store_true',
            dest='mp_split_all',
            default=env.get('NOSE_MP_SPLIT_ALL', None),
            help='Add `_multiprocess_can_split_ = True` to all test modules '
                 'and classes. This lets the multiprocess module distribute '
                 'test functions/methods across processes, rather than just '
                 'test classes. Defaults to False (disabled). '
                 '[NOSE_MP_SPLIT_ALL]',
        )

    def configure(self, options, conf):
        super(MpSplitPlugin, self).configure(options, conf)
        self.enabled = options.mp_split_all
        if not self.enabled:
            return

        self.mp_split_all = options.mp_split_all

    def wantModule(self, module):
        if self.mp_split_all:
            module._multiprocess_can_split_ = True

    def wantClass(self, cls):
        if self.mp_split_all:
            cls._multiprocess_can_split_ = True