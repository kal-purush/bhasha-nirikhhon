#
#    Modular REST API dispatcher in Python (dispytch)
#
#    Copyright (C) 2015 Denis Pompilio (jawa) <denis.pompilio@gmail.com>
#    Copyright (C) 2015 Cyrielle Camanes (cycy) <cyrielle.camanes@gmail.com>
#
#    This file is part of dispytch
#
#    This program is free software; you can redistribute it and/or
#    modify it under the terms of the GNU General Public License
#    as published by the Free Software Foundation; either version 2
#    of the License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, see <http://www.gnu.org/licenses/>.


def merge_dict(dict_a, dict_b):
    """Utility function to recusively merge two dictionnaries

    :param dict dict_a: Initial dictionnary to update
    :param dict dict_b: Dictionnary containing updated keys

    :return: Merged dictionnary
    :rtype: dict
    """
    if not isinstance(dict_b, dict):
        return dict_b

    for key, val in dict_b.items():
        if key in dict_a and isinstance(dict_a[key], dict):
            dict_a[key] = merge_dict(dict_a[key], val)
        else:
            dict_a[key] = val

    return dict_a