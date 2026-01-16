# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

from blinker import NamedSignal


context_initialized = NamedSignal('context_initialized')
pre_context_changed = NamedSignal('context_changed')
post_context_changed = NamedSignal('context_changed')
context_key_changed = NamedSignal('context_key_changed')