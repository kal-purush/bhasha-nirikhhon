module blogPosts {
  'use strict';

  export class BlogPostCtrl {
    private posts = [
        new BlogPost("First Post", "This is the body"),
        new BlogPost("Second Post", "This is the body"),
        new BlogPost("Third Post", "This is the body"),
        new BlogPost("Fourth Post", "This is the body")
    ];

    public static $inject = [
			'$scope',
			'$location'
		];

    constructor(
      private $scope,
      private $location: ng.ILocationService
    ) {
      $scope.vm = this;


    }

  }

}