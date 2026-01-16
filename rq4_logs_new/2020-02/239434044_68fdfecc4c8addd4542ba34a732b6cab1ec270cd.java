package controller;

public class ChangePwdCommand {

		private String currentPassword;
		private String newPassword;
		
		public String getCurrentPassword() {
			return currentPassword;
		}

		public String getNewPassword() {
			return newPassword;
		}

		public void setNewPassword(String newPassword) {
			this.newPassword = newPassword;
		}

		public void setCurrentPassword(String currentPassword) {
			this.currentPassword = currentPassword;
		}
		
}