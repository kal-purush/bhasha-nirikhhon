from __future__ import print_function
from release_ccharp.apps.common.single_file_read_write import BinaryVersionUpdater
from release_ccharp.apps.common.base import ApplicationBase
from release_ccharp.apps.common.directory_handling import FileDeployer
from release_ccharp.apps.order_scripts.builder import OrderBuilder
from release_ccharp.apps.order_scripts.validation_deployer import OrderValidationDeployer
from release_ccharp.apps.order_scripts.deployer import OrderDeployer
from release_ccharp.snpseq_paths import SnpseqPathActions


class Application(ApplicationBase):
    """
    Code that is specific to the Chiasma application
    It needs snpseq-workflow because the latest and candidate version
    is fetched through the github provider (in the workflow)
    """
    def __init__(self, snpseq_workflow, branch_provider, os_service, windows_commands, whatif):
        super(Application, self).__init__(snpseq_workflow, branch_provider, os_service,
                                          windows_commands, whatif)
        self.binary_version_updater = BinaryVersionUpdater(
            whatif=False, config=self.config, path_properties=self.path_properties,
            branch_provider=branch_provider, app_paths=self.app_paths, os_service=os_service)
        file_deployer = FileDeployer(
            self.path_properties, self.os_service, snpseq_workflow.config, self.app_paths)
        self.order_builder = OrderBuilder(self, file_deployer, self.app_paths, self.config)
        path_actions = SnpseqPathActions(
            whatif, self.path_properties, os_service, self.app_paths, self.windows_commands)
        self.validation_deployer = OrderValidationDeployer(
            self, file_deployer, path_actions)
        self.deployer = OrderDeployer(
            self.path_properties, file_deployer, path_actions, branch_provider)

    def build(self):
        super(Application, self).build()
        self.order_builder.run()

    def deploy_validation(self):
        super(Application, self).deploy_validation()
        self.validation_deployer.run()

    def deploy(self, skip_copy_backup):
        super(Application, self).deploy(skip_copy_backup)
        self.deployer.run(skip_copy_backup)

    def download_release_history(self):
        super(Application, self).download_release_history()
        self.snpseq_workflow.download_release_history()
        self.deployer.copy_release_history()