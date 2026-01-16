/// <reference path="HtmlBuilder" />
/// <reference path="printapi/Pullrequest" />

module PrintClient {
	export class PullrequestBuilder {
		static createPRBox(pr: PrintApi.Pullrequest, listView: boolean) {
			var container = HtmlBuilder.createElement("article", "", "id", pr.id);
			var content: Element;
			if (listView)
				content = PullrequestBuilder.buildListViewPRBox(pr);
			else
				content = PullrequestBuilder.buildPRBox(pr);
			container.appendChild(content);
			return container;
		}
		static updatePRBox(pr: PrintApi.Pullrequest, listView: boolean) {
			var success: boolean = false;
			var pr_container = document.getElementById(pr.id);
			if (pr_container) {
				pr_container.innerHTML = "";
				var content: Element;
				if (listView)
					content = PullrequestBuilder.buildListViewPRBox(pr);
				else
					content = PullrequestBuilder.buildPRBox(pr);
				pr_container.appendChild(content);
				success = true;
			}
			return success;
		}
		static buildListViewPRBox(pr: PrintApi.Pullrequest) {
			var main = HtmlBuilder.createElement("main");
			var title = HtmlBuilder.createElement("a", "", "href", "/print/" + pr.repositoryName + "/pr/" + pr.id);
			title.setAttribute("onclick", "printClient.loadPR(event, this)");
			title.appendChild(HtmlBuilder.createElement("h2", pr.title + " #" + String(pr.number)));
			main.appendChild(title);
			main.appendChild(PullrequestBuilder.createStatusIconForList(pr.executionResults));
			main.appendChild(HtmlBuilder.createElement("p", "Created by " + pr.user.username));
			main.appendChild(HtmlBuilder.createElement("p", pr.description));
			return main;
		}
		static buildPRBox(pr: PrintApi.Pullrequest) {
			var main = HtmlBuilder.createElement("main");
			main.appendChild(HtmlBuilder.createElement("p", pr.description));
			main.appendChild(HtmlBuilder.createElement("p", "Execution status:"));
			pr.executionResults.forEach((result) => {
				main.appendChild(HtmlBuilder.createElement("p", result.task, "style", "display:inline-block;margin-right:10px;"));
				main.appendChild(PullrequestBuilder.createStatusIcon(result.result));
				main.appendChild(HtmlBuilder.createElement("br"));
			});
			return main;
		}
		static createStatusIconForList(executionResults: PrintApi.ExecutionResult[]) {
			var icon = "octicon-server";
			for (var i = 0; i < executionResults.length; i++) {
				icon = "octicon-check";
				if (executionResults[i].result != "OK") {
					icon = "octicon-x";
					i = executionResults.length;
				}
			}
			return HtmlBuilder.createElement("span", "", "class", "octicon " + icon);
		}
		static createStatusIcon(result: string) {
			var icon = "octicon-check";
			if (result != "OK")
				icon = "octicon-x";
			return HtmlBuilder.createElement("span", "", "class", "octicon " + icon);
		}
	}
}