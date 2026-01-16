package com.strandls.mail.service.impl;

import java.util.List;

import javax.inject.Inject;

import com.strandls.mail.model.MailInfo;
import com.strandls.mail.service.DocumentMailService;
import com.strandls.mail.util.ThreadUtil;
import com.strandls.mail.util.AppUtil.TEMPLATE;

public class DocumentMailServiceImpl implements DocumentMailService {

	@Inject
	private ThreadUtil threadUtil;

	@Override
	public void sendDocumentPostToGroupMail(List<MailInfo> info) {
		String submitType = info.get(0).getData().get("submitType").toString();
		String subject;
		if (submitType.equalsIgnoreCase("post")) {
			subject = "Posted document to group";
		} else {
			subject = "Removed document from group";
		}

		threadUtil.startThread(TEMPLATE.DOCUMENT.getValue(), subject, info);

	}

	@Override
	public void sendDocumentCommentedMail(List<MailInfo> info) {
		threadUtil.startThread(TEMPLATE.DOCUMENT.getValue(), "New comment in document", info);

	}

	@Override
	public void sendDocumentAddedMail(List<MailInfo> info) {
		threadUtil.startThread(TEMPLATE.DOCUMENT.getValue(), "Document added", info);

	}

	@Override
	public void sendDocumentFlaggedMail(List<MailInfo> info) {
		threadUtil.startThread(TEMPLATE.DOCUMENT.getValue(), "Document flagged", info);

	}

	@Override
	public void sendDocumentUpdatedMail(List<MailInfo> info) {
		threadUtil.startThread(TEMPLATE.DOCUMENT.getValue(), "Document updated", info);

	}

	@Override
	public void sendDocumentDeletedMail(List<MailInfo> info) {
		threadUtil.startThread(TEMPLATE.DOCUMENT.getValue(), "Document deleted", info);

	}

}