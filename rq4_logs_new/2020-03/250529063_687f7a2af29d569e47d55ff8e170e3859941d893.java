package opcon.commvault.connector.arguments;

import com.beust.jcommander.Parameter;

public class CommVaultArguments {

	private static final String CommVaultXMLTemplateFileDescriptionMsg = "(Required) XML Template file to use to start Commvault Job";
	private static final String CommVaultBackupsetNameDescriptionMsg = "(Required) Backupset Name";
	private static final String CommVaultClientNameDescriptionMsg = "(Required) Client Name";
	private static final String CommVaultSubClientNameDescriptionMsg = "(Optional) SubClient Name";
	private static final String CommVaultInstanceDescriptionMsg = "(Optional) Instance";
	private static final String CommVaultBackupTypeDescriptionMsg = "(Required) Backup Type - FULL / INCR";
	
	@Parameter(names="-bsn", required=true, description = CommVaultBackupsetNameDescriptionMsg)
	private String commVaultBackupsetName = null;

	@Parameter(names="-xt", required=true, description = CommVaultXMLTemplateFileDescriptionMsg)
	private String commVaultXmlTemplate = null;
	
	@Parameter(names="-c", required=true, description = CommVaultClientNameDescriptionMsg)
	private String commVaultClientName = null;

	@Parameter(names="-sc", required=true, description = CommVaultSubClientNameDescriptionMsg)
	private String commVaultSubClientName = null;

	@Parameter(names="-i", required=true, description = CommVaultInstanceDescriptionMsg)
	private String commVaultInstance = null;

	@Parameter(names="-t", required=true, description = CommVaultBackupTypeDescriptionMsg)
	private String commVaultBackupType = null;

	public String getCommVaultBackupsetName() {
		return commVaultBackupsetName;
	}

	public void setCommVaultBackupsetName(String commVaultBackupsetName) {
		this.commVaultBackupsetName = commVaultBackupsetName;
	}

	public String getCommVaultXmlTemplate() {
		return commVaultXmlTemplate;
	}

	public void setCommVaultXmlTemplate(String commVaultXmlTemplate) {
		this.commVaultXmlTemplate = commVaultXmlTemplate;
	}

	public String getCommVaultClientName() {
		return commVaultClientName;
	}

	public void setCommVaultClientName(String commVaultClientName) {
		this.commVaultClientName = commVaultClientName;
	}

	public String getCommVaultSubClientName() {
		return commVaultSubClientName;
	}

	public void setCommVaultSubClientName(String commVaultSubClientName) {
		this.commVaultSubClientName = commVaultSubClientName;
	}

	public String getCommVaultInstance() {
		return commVaultInstance;
	}

	public void setCommVaultInstance(String commVaultInstance) {
		this.commVaultInstance = commVaultInstance;
	}

	public String getCommVaultBackupType() {
		return commVaultBackupType;
	}

	public void setCommVaultBackupType(String commVaultBackupType) {
		this.commVaultBackupType = commVaultBackupType;
	}

}