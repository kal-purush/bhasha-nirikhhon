#Test cases for task-cli program

import pytest
import actions

class TestActions:

    def test_add():
        actions.add_task()
