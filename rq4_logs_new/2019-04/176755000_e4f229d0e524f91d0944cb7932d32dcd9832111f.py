#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path

from click.testing import CliRunner


from plugins.windowraiser import WindowItemList, WindowRaiser, WindowItem
from yeahyeah.core import YeahYeah


def test_window_raiser_no_config_file(tmpdir):
    plugin = WindowRaiser(item_list=[])
    plugin.assert_config_file(Path(tmpdir) / "window_raiser_config_test.txt")


def test_window_raiser_plugin():

    yeahyeah = YeahYeah()
    plugin = WindowRaiser(
        item_list=WindowItemList([WindowItem(name="test", pattern="testpattern")])
    )
    yeahyeah.add_plugin(plugin)
    runner = CliRunner()
    response = runner.invoke(
        yeahyeah.root_cli, "test"
    )
    assert response.exit_code == 0




