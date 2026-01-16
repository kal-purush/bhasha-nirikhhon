package org.eurofurence.regsys.web.pages;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.eurofurence.regsys.backend.Constants;
import org.eurofurence.regsys.backend.Strings;
import org.eurofurence.regsys.backend.persistence.DbDataException;
import org.eurofurence.regsys.backend.persistence.DbException;
import org.eurofurence.regsys.repositories.attendees.Attendee;
import org.eurofurence.regsys.repositories.errors.DownstreamException;
import org.eurofurence.regsys.repositories.errors.DownstreamWebErrorException;
import org.eurofurence.regsys.repositories.payments.PaymentService;
import org.eurofurence.regsys.repositories.payments.Transaction;
import org.eurofurence.regsys.repositories.payments.TransactionResponse;
import org.eurofurence.regsys.web.forms.Form;
import org.eurofurence.regsys.web.forms.FormHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 *  Represents the sepa payment information page in the registration system.
 *
 *  Regular attendees may use it to obtain money transfer information and tell us that they've transferred.
 *
 *  This page understands these request parameters:
 *      transaction             payment reference id
 *      sent                    (button caption)
 *      back                    (button caption)
 *
 *  This page uses these forms:
 *      NavbarForm (with menu disabled)
 */
public class SepaPage extends Page {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    // constants for parameter names
    public static final String TRANSACTION = "transaction";
    public static final String SUBMIT_SENT = "sent";
    public static final String SUBMIT_BACK = "back";

    private String refId = "";
    private String subject = "";
    private boolean showInfo = false;
    private Transaction transaction = new Transaction();
    private final PaymentService paymentService = new PaymentService();

    // page accessors

    @SuppressWarnings("unused")
    public String getAmount() {
        return FormHelper.toCurrencyDecimals(transaction.amount.grossCent) + "&nbsp;" + transaction.amount.currency;
    }

    @SuppressWarnings("unused")
    public String getSubject() {
        return subject;
    }

    @SuppressWarnings("unused")
    public String getRefId() {
        return refId;
    }

    @SuppressWarnings("unused")
    public String getAccountOwner() {
        return getConfiguration().sepa.accountOwner;
    }

    @SuppressWarnings("unused")
    public String getBankName() {
        return getConfiguration().sepa.bankName;
    }

    @SuppressWarnings("unused")
    public String getBankAddress() {
        return getConfiguration().sepa.bankAddress;
    }

    @SuppressWarnings("unused")
    public String getIban() {
        String iban = getConfiguration().sepa.iban;

        try {
            String spacedIban = iban.substring(0, 4) + " "
                    + iban.substring(4, 8) + " "
                    + iban.substring(8, 12) + " "
                    + iban.substring(12, 16) + " "
                    + iban.substring(16, 20) + " "
                    + iban.substring(20);
            return spacedIban + " (" + iban + ")";
        } catch (Exception ignore) {
        }

        return iban;
    }

    @SuppressWarnings("unused")
    public String getBic() {
        String bic = getConfiguration().sepa.bic;

        try {
            String spacedBic = bic.substring(0, 4) + " "
                    + bic.substring(4, 8) + " "
                    + bic.substring(8);
            return spacedBic + " (" + bic + ")";
        } catch (Exception ignore) {
        }

        return bic;
    }

    @SuppressWarnings("unused")
    public boolean mayView() {
        return showInfo;
    }

    // form
    @SuppressWarnings("unused")
    public String getFormHeader() {
        return "<FORM ACTION='sepa' METHOD='post' accept-charset='UTF-8'>" +
                Form.hiddenField(TRANSACTION, transaction.transactionIdentifier);
    }

    @SuppressWarnings("unused")
    public String getFormFooter() {
        return "</FORM>";
    }

    @SuppressWarnings("unused")
    public String submitSent(String caption, String style) {
        return "<INPUT TYPE='SUBMIT' NAME='" + SUBMIT_SENT + "' "
                + "VALUE='" + Form.escape(caption) + "' class='" + Form.escape(style) + "'/>";
    }

    @SuppressWarnings("unused")
    public String submitBack(String caption, String style) {
        return "<INPUT TYPE='SUBMIT' NAME='" + SUBMIT_BACK + "' "
                + "VALUE='" + Form.escape(caption) + "' class='" + Form.escape(style) + "'/>";
    }

    // flow control

    @Override
    public void initialize(ServletContext servletContext, HttpServletRequest request, HttpServletResponse response, HttpSession session) {
        super.initialize(servletContext, request, response, session);
    }

    @Override
    public String handleRequest() throws ServletException {
        refreshSessionTimeout();

        if (!isLoggedIn()) {
            return redirect("page/start");
        }

        if (!isRegistrationEnabled()) {
            return forward("page/start");
        }

        if (getRequest().getParameter(SUBMIT_BACK) != null) {
            return redirectExternal(getConfiguration().sepa.failureRedirect);
        }

        try {
            Constants.MemberStatus status = getLoggedInAttendeeStatus();
            if (status == Constants.MemberStatus.APPROVED || status == Constants.MemberStatus.PARTIALLY_PAID) {
                Attendee me = getLoggedInAttendee();

                refId = getRequest().getParameter(TRANSACTION);
                if (refId == null || "".equals(refId)) {
                    throw new DbDataException(Strings.sepaPage.transactionNotFound);
                }

                logger.info("reading transaction " + refId);

                TransactionResponse response = paymentService.performFindTransactions(null, refId, null, null, getTokenFromRequest(), getRequestId());
                if (response == null || response.payload == null || response.payload.size() != 1) {
                    throw new DbDataException(Strings.sepaPage.transactionNotFound);
                }
                transaction = response.payload.get(0);

                if (!longEquals(me.id, transaction.debitorId)) {
                    throw new DbDataException(Strings.sepaPage.wrongAttendee);
                }
                if (!"payment".equals(transaction.transactionType) || !"transfer".equals(transaction.method)) {
                    throw new DbDataException(Strings.sepaPage.transactionNotEligible);
                }
                if (!"tentative".equals(transaction.status)) {
                    throw new DbDataException(Strings.sepaPage.transactionWrongStatus);
                }

                if (getRequest().getParameter(SUBMIT_SENT) != null) {
                    logger.info("setting transaction " + transaction.transactionIdentifier + " to pending");

                    transaction.status = "pending";
                    paymentService.performUpdateTransaction(transaction.transactionIdentifier, transaction, getTokenFromRequest(), getRequestId());

                    return redirectExternal(getConfiguration().sepa.successRedirect);
                }

                subject = getConfiguration().sepa.subjectPrefix + " " + me.nickname + " " + me.id.toString();
                showInfo = true;
            } else if (status == Constants.MemberStatus.PAID || status == Constants.MemberStatus.CHECKED_IN) {
                throw new DbDataException(Strings.sepaPage.nothingToPay);
            } else {
                throw new DbDataException(Strings.sepaPage.wrongStatus);
            }
        } catch (DbException e) {
            addError(e.getMessage());
        } catch (DownstreamWebErrorException e) {
            resetErrors();
            addError(Strings.sepaPage.backendError);
            addWebErrors(e.getErr());
        } catch (DownstreamException e) {
            resetErrors();
            addError(e.getMessage());
        }

        getNavbarForm().disableMenu();

        HashMap<String, Object> veloContext = new HashMap<>();
        veloContext.put("page", this);
        veloContext.put("navbar", getNavbarForm());

        return Page.renderTemplate(getServletContext(), "html.vm", veloContext);
    }

    @Override
    protected String getPageTitle() {
        return Strings.sepaPage.pageTitle;
    }

    @Override
    public String getPageTemplateFile() {
        return "sepa.vm";
    }

    private boolean longEquals(Long a, Long b) {
        if (a == null || b == null) {
            return false;
        }
        return a.longValue() == b.longValue();
    }
}