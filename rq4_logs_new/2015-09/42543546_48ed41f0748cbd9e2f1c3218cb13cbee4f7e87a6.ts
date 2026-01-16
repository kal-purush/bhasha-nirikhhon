import "angular";
import "angular-ui-router";
import "angular-animate";

require('../node_modules/bootstrap/dist/css/bootstrap.css');
require('./app.less');

import repoStore from "./repo-store/repo-store.service";
import repos from "./app/repos/repos.component";
import repoList from "./app/repo-list/repo-list.component";
import repoDetail from "./app/repo-detail/repo-detail.component";
import marked from "./app/marked/marked.component";
import router from "./app/app.config.router";

const app = angular.module("app", ["ngAnimate", "ui.router"])
	.service("repoStore", repoStore)
	.directive("repos", repos)
	.directive("repoList", repoList)
	.directive("repoDetail", repoDetail)
	.directive("marked", marked);

app.config(router);

export default app;