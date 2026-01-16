/**
 * Created by vidyachandasekhar on 5/7/17.
 */
public class Result {
    private String failureTestDetails;

    private String successTestsDetails;

    public String getSuccessTestsDetails() {
        return successTestsDetails;
    }

    public void setSuccessTestsDetails(String successTestsDetails) {
        this.successTestsDetails = successTestsDetails;
    }

    public String getFailureTestDetails() {
        return failureTestDetails;
    }

    public void setFailureTestDetails(String failureTestDetails) {
        this.failureTestDetails = failureTestDetails;
    }

    @Override
    public String toString() {
        if (failureTestDetails != null) {
            return failureTestDetails ;
        } else {
            return successTestsDetails;
        }

    }
}