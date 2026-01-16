from ftw.upgrade import UpgradeStep


class AddIframeblockJsForIfrageLoader(UpgradeStep):
    """Add iframeblock js for ifrage loader.
    """

    def __call__(self):
        self.install_upgrade_profile()