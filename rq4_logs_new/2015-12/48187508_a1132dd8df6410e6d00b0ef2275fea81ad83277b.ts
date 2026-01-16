module app.movies {
    class Movie {
        constructor(private title: string, private year: number, private rating?: number) {
            this.rating = rating || 0;
        }
    }
    export class MovieList {
        static $inject: Array<string> = ['$scope'];

        constructor(private $scope: ng.IScope) {
            var movies: Array<Movie> = [];
            movies.push(new Movie("Goodfellas", 1990, 5));
            movies.push(new Movie("Casino", 1995, 5));
            movies.push(new Movie("Taxi Driver", 1976, 5));
            movies.push(new Movie("Raging Bull", 1980, 4));
            movies.push(new Movie("Gangs of New York", 2002, 3));
            $scope['movies'] = movies;
        }
    }
}

angular.module('app').controller('movieList', app.movies.MovieList);