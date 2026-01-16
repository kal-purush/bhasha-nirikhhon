"""
Processes parsed data to be in the form:
keywords -> data desired
"""
import cPickle, os

from blank.data_utils import OUTPUT_DIR, ELEMENT_MAPPING
from blank.data_utils import cache

def load(name):
    filepath = os.path.join(OUTPUT_DIR, name)
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'rb') as f:
        return cPickle.load(f)

def lower_keys(_dict):
    return dict((key.lower(), value) for key, value in _dict.iteritems())

def update_dict(_dict, key, values):
    orig = _dict.get(key, {})
    orig.update(values)
    _dict[key] = orig

def set_value(_dict, keys, value):
    for key in keys:
        _dict[key] = value

def merge_dicts(ionization, isotopes):
    element_data = {}

    # Processes Ionization Levels
    for elem, ion in ionization.iteritems():
        symbol = ELEMENT_MAPPING[elem].lower()
        ion = lower_keys(ion)
        ion['protons'] = ion['atomic number']
        element_data[symbol] = ion

        for key, value in ion.iteritems():
            orig = element_data.get(key, {})
            orig[symbol] = value
            element_data[key] = orig

    # Process Isotopes
    for elem, isotope in isotopes.iteritems():
        symbol = ELEMENT_MAPPING[elem].lower()
        isotope = lower_keys(isotope)
        if "standard atomic weight" in isotope:
            set_value(
                element_data[symbol],
                [ "atomic weight", "atomic mass", "mass", "amu", "weight"],
                isotope["standard atomic weight"]
            )
        isotope_dict = {}
        for each in isotope["isotopes"]:
            each = lower_keys(each)
            set_value(
                each,
                [ "atomic weight", "atomic mass", "mass", "amu", "weight"],
                each["relative atomic mass"]
            )
            mass_number = each["mass number"]
            if symbol in element_data:
                element_data[symbol][mass_number] = each
            update_dict(element_data, mass_number, {symbol: each})
            isotope_dict[mass_number] = each

        if symbol in element_data:
            element_data[symbol]["isotope"] = isotope_dict
        update_dict(element_data, symbol, isotope_dict)
    return element_data


if __name__ == "__main__":
    cache(merge_dicts(
        load("atomic_ionization_output.p"),
        load("atomic_weight_compositions.p")
    ), os.path.join(OUTPUT_DIR, "element_data.p"))