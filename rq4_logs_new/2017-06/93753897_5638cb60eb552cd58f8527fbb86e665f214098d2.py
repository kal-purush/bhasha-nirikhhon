# /usr/bin/python
# Copyright (C) 2017 Eitan Geiger and Sebastian Scheinkman
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.


class AbstractPolicyApplication(object):
    def __init__(self, managed_endpoint):
        self.managed_endpoint = managed_endpoint

    def apply(self):
        if self.managed_endpoint.strict:
            self.clean()

    def clean(self):
        pass