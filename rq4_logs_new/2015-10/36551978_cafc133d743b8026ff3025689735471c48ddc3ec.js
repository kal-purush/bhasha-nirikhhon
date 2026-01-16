describe( 'posts.ctrl', function() {
    beforeEach( module( 'app' ) )
    var $scope
    
    var mockPostsSvc = {}
    beforeEach( inject( function( $q ) {
        mockPostsSvc.fetch = function() {
            var deferred = $q.defer()
            deferred.resolve([
                { username: 'spirift', body: 'first post' },
                { username: 'spirift', body: 'second post' }
            ])
            return deferred.promise
        }
    }))
    
    beforeEach( inject( function( $rootScope, $controller ) {
        $scope = $rootScope.$new()
        $controller( 'PostsCtrl', {
            $scope: $scope,
            PostsSvc: mockPostsSvc
        })
    }))
    
    it( 'loads posts from the service  ', function() {
        $scope.$digest()
        expect( $scope.posts ).to.have.length( 2 )
    })
})