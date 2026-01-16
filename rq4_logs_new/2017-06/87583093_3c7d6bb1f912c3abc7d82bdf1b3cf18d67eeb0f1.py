"""
===============================
test_rc_airshow_default_disable
===============================

:Module Desc:   Verify the Airshow feature is disabled after restoring factory
                default settings.
                

:Test Reviewer:
:Req Coverage:  Full


"""

import pytest

pytestmark = [pytest.mark.req("PLAT-2378"),
              pytest.mark.author("jherrin"),
              pytest.mark.t_area("ANY"),
              pytest.mark.mat3]


def setup_module():

    """
    Setup for all tests in this module

    Module Setup Steps:

    #. Connect a Test PC to the Maintenance port (or Eth10) of the LRU
    #. Open a browser on the test laptop and Navigate to wizard.gogo.aero, or 
    #. enter the IP address 172.20.1.1
    #. Login to the maintenance page with username "gogo" and password "gogopassword" 
    
    #. Note: This module should be run on an LRU that was previously configured for
    #.       RC Airshow enabled.

    """

    pass


def setup_function():

    """
    Setup for every test case in this module

    Function Setup Steps:

    #. Navigate to the Features Active Status page
    #. Verify the Rockwell Collins Airshow App shows "Enabled"
    #. Navigate to the Equipment Configuration page
    #. Factory reset is on Manage System page
    """

    pass


def teardown_function():

    """
    Teardown to be run after every test case

    Function Teardown Steps:

    #. Close the browser window on the Test PC.
    #. Remove connection between the Test PC and the maintenance port on the LRU.

    """
    pass


def teardown_module():
    """
    Teardown after all tests in this module

    Module Teardown Steps:

    #. Since the test function just did a Factory Default, nothing to do here.

    """

    pass
    
@pytest.mark.mat3
@pytest.mark.t_type("Normal")
def test_rc_airshow_default_disable():

    """
    :Test Description: PLAT-2378 The LRU shall disable Rockwell Collins Airshow feature on the cabin bridge by default.
                       

    **Setup:**

        #. Execute the steps from :func:`setup_module`
        #. Execute the steps from :func:`setup_function`


    **Teststeps:**

        #. From the laptop browser, navigate to the Manage System tab of the GUI
        #. In the Restore Factory Defaults tile, click on the Reset button
        #. In the dialog box that appears, click Confirm to initiate the reboot sequence
        #. When the system reboots (approximately 90 seconds), log into the LRU again
        #. In the browser of the Test PC, navigate to the Features Active page
        #. Verify that the Rockwell Collins Airshow App shows Disabled
        

    **Teardown:**

        #. Execute the steps from :func:`teardown_function`
        #. Execute the steps from :func:`teardown_module`

    """
    pass