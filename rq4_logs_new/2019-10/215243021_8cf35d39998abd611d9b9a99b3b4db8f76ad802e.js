const clientID = 'Iv1.c5a94128b48f972d';
const clientSecret = 'c3576d6bd5166a63b92e42cfcfa3081417f36c6f';

// Everytime A New Character is typed - 'username' will hold the entire new string
$(document).ready(function() {
	$('#searchUser').on('keyup', function(e) {
		let username = e.target.value;

		//GitHub Request

		$.ajax({
			url: 'https://api.github.com/users/' + username,
			data: {
				access_token: '66b57cbe57bc9e3cd8c049e10585222c16c67d11',
				client_id: clientID,
				client_secret: clientSecret
			}
		}).done(function(user) {
			$.ajax({
				url: 'https://api.github.com/users/' + username + '/repos',
				data: {
					access_token: '66b57cbe57bc9e3cd8c049e10585222c16c67d11',
					client_id: clientID,
					client_secret: clientSecret,
					sort: 'created: asc',
					per_page: 5
				}
			}).done(function(repos) {
				$.each(repos, function(index, repo) {
					$('#repos').append(`
                    <div class="well">
                        <div class="row">
                            <div class="col-md-7">
                                <div class="card card-body bg-light">
                                    <strong>${repo.name}</strong>: ${repo.description}
                                </div>
                            </div>
                            <div class="col-md-3">
                                <div class="card card-body bg-light">
                                    <span class="badge badge-pill badge-primary">Forks: ${repo.forks_count}</span>
                                    <span class="badge badge-pill badge-danger">Watchers: ${repo.watchers_count}</span>
                                    <span class="badge badge-pill badge-success">Stars: ${repo.stargazers_count}</span>
                                </div>
                            </div>
                            <div class="col-md-2">
                                <a href="${repo.html_url}" target="_blank" class="btn btn-primary">Visit Repo</a>
                            </div>
                        </div>
                    </div>
                `);
				});
			});
			$('#profile').html(`
                <div class="card">
                    <div class="card-header">
                        <h5 class="card-title">${user.name}</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <img class="thumbnail avatar" src="${user.avatar_url}">
                                <a href="${user.html_url}" class="btn btn-primary btn-block" target="_blank">Visit Profile</a>
                            </div>
                            <div class="col-md-9">
                                <span class="badge badge-pill badge-primary">Public Repos: ${user.public_repos}</span>
                                <span class="badge badge-pill badge-danger">Public Gists: ${user.public_gists}</span>
                                <span class="badge badge-pill badge-success">Followers: ${user.followers}</span>
                                <span class="badge badge-pill badge-info">Following: ${user.following}</span>
                                <br><br>
                                <ul class="list-group">
                                    <li class="list-group-item">Company: ${user.company}</li>
                                    <li class="list-group-item">Website/Blog: ${user.blog}</li>
                                    <li class="list-group-item">Location: ${user.location}</li>
                                    <li class="list-group-item">Member Since: ${user.created_at}</li>
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>

                <h1>Latest Repositories</h1>
                <div id="repos">
               
                </div>
            `);
		});
	});
});