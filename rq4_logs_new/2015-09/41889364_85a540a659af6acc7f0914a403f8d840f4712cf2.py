

import openerp

from openerp import api
from openerp.api import one, multi

from openerp.exceptions import Warning as UserError
from openerp.exceptions import Warning as OtherName
from openerp.exceptions import Warning
from openerp.exceptions import AccessError as AE, \
    ValidationError, Warning as UserError2


class snake_case(object):
    pass


class UseUnusedImport(object):
    def method1(self):
        return UserError, OtherName, Warning, AE, ValidationError, UserError2


class ApiOne(object):
    @api.one
    def copy():
        pass


class One(object):
    @one
    def copy():
        pass


class OpenerpApiOne(object):
    @openerp.api.one
    def copy():
        pass


class WOApiOne(object):
    # copy without api.one decorator
    def copy():
        pass


class ApiOneMultiTogether(object):

    @api.multi
    @api.one
    def copy():
        pass

    @multi
    @one
    def copy2():
        pass

    @openerp.api.multi
    @openerp.api.one
    def copy3():
        pass

# vim:comment vim