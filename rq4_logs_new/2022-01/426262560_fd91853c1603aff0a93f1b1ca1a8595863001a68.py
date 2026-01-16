from largo.project import Project
from largo.date_range import DateRange
from largo.ledger_invoke import LedgerInvoke
from typing import List
import datetime


class CashFlow(LedgerInvoke):
    def __init__(self, project: Project, date_range: DateRange):
        self._project = project
        self._date_range = date_range

    @property
    def project(self) -> Project:
        return self._project

    @property
    def year(self) -> int:
        return self._date_range.year

    @property
    def ledger_subcommand(self) -> str:
        return 'balance'

    @property
    def accounts(self) -> List[str]:
        return self._project.account.cash

    @property
    def period_begin(self) -> datetime.date:
        return self._date_range.begin

    @property
    def period_end(self) -> datetime.date:
        return self._date_range.end

    @property
    def default_options(self) -> List[str]:
        if settings := self.project.cf_command:
            return settings.default_options
        else:
            return []