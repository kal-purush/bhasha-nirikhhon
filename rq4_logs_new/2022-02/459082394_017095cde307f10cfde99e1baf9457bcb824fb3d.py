#!/usr/bin/env python3.7

from re import sub
import iterm2
from iterm2.util import CoordRange,WindowedCoordRange
from iterm2.selection import SubSelection,SelectionMode
import subprocess
# This script was created with the "basic" environment which does not support adding dependencies
# with pip.


def copy_clipboard(text):
    pr1=subprocess.Popen(["echo","-n",text],stdout=subprocess.PIPE)
    subprocess.Popen("pbcopy",stdin=pr1.stdout)
    return True

async def main(connection):
    @iterm2.RPC
    async def copy_with_line():
        app = await iterm2.async_get_app(connection)
        session=app.current_window.current_tab.current_session
        selection=await session.async_get_selection()
        text=await session.async_get_selection_text(selection)
        if text is None or text=="":#選択テキストがないとき
            prompt=await iterm2.async_get_last_prompt(connection,session.session_id)
            if prompt:
                text=prompt.command#コマンドライン行を読み取る。
            else:
                pass
        copy_clipboard(text)
    await copy_with_line.async_register(connection)


iterm2.run_forever(main)