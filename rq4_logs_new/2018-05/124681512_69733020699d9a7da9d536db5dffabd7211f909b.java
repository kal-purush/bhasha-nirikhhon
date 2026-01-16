package polls;

import java.util.List;

public class Result {
	private String pollId;
	private List<String> answerIds;

	public Result() {
	}

	public String getPollId() {
		return pollId;
	}

	public List<String> getAnswerIds() {
		return answerIds;
	}
}