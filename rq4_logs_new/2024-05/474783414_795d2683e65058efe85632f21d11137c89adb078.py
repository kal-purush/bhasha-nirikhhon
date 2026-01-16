r"""Contains tests for the module :mod:`czekitout.name`.

"""



#####################################
## Load libraries/packages/modules ##
#####################################

# For general array handling.
import numpy as np



# For getting fully qualified class names.
import czekitout.name



############################
## Authorship information ##
############################

__author__     = "Matthew Fitzpatrick"
__copyright__  = "Copyright 2024"
__credits__    = ["Matthew Fitzpatrick"]
__maintainer__ = "Matthew Fitzpatrick"
__email__      = "mrfitzpa@uvic.ca"
__status__     = "Development"



##################################
## Define classes and functions ##
##################################



def test_1_of_fully_qualified_class_name():
    module_alias = czekitout.name
    func_alias = module_alias.fully_qualified_class_name

    obj_set = ([1, 2],
               np.random.default_rng(),
               sum)
    expected_result_set = ("list",
                           "numpy.random._generator.Generator",
                           "builtin_function_or_method")
    zip_obj = zip(obj_set, expected_result_set)
    for obj, expected_result in zip_obj:
        assert func_alias(obj) == expected_result

    cls_set = (tuple,
               np.polynomial.polynomial.Polynomial)
    expected_result_set = ("tuple",
                           "numpy.polynomial.polynomial.Polynomial")
    zip_obj = zip(cls_set, expected_result_set)
    for cls, expected_result in zip_obj:
        assert func_alias(cls) == expected_result



###########################
## Define error messages ##
###########################