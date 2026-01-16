import logging

from django.conf import settings
from django.core.cache import cache
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import generics, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from core.models import Genre, Movie
from core.serializers import GenreSerializer, MovieListSerializer
from core.utils.tmdb import TMDBClient
from core.views.movies import StandardResultsSetPagination

# Initialize TMDB client
tmdb_client = TMDBClient(api_key=settings.TMDB_API_KEY)

# Configure logger
logger = logging.getLogger(__name__)


class GenreListView(generics.ListAPIView):
    """
    List all available movie genres.
    """

    serializer_class = GenreSerializer
    permission_classes = [AllowAny]
    queryset = Genre.objects.all().order_by("name")

    @swagger_auto_schema(
        operation_description="Get all movie genres",
        responses={200: GenreSerializer(many=True)},
    )
    @method_decorator(cache_page(60 * 60 * 24))  # Cache for 24 hours
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)


class GenreMoviesView(generics.ListAPIView):
    """
    List movies by genre.
    """

    serializer_class = MovieListSerializer
    pagination_class = StandardResultsSetPagination
    permission_classes = [AllowAny]

    @swagger_auto_schema(
        operation_description="Get movies by genre",
        manual_parameters=[
            openapi.Parameter(
                "page", openapi.IN_QUERY, description="Page number", type=openapi.TYPE_INTEGER, default=1
            ),
        ],
        responses={
            200: openapi.Response(
                description="Successful response",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "results": openapi.Schema(type=openapi.TYPE_ARRAY, items=MovieListSerializer()),
                        "genre": GenreSerializer(),
                        "page": openapi.Schema(type=openapi.TYPE_INTEGER),
                        "total_pages": openapi.Schema(type=openapi.TYPE_INTEGER),
                        "total_results": openapi.Schema(type=openapi.TYPE_INTEGER),
                        "next": openapi.Schema(type=openapi.TYPE_INTEGER, nullable=True),
                        "previous": openapi.Schema(type=openapi.TYPE_INTEGER, nullable=True),
                    },
                ),
            ),
            400: "Bad Request - Invalid genre ID",
            404: "Genre not found",
        },
    )
    @method_decorator(cache_page(60 * 60))  # Cache for 1 hour
    def get(self, request, *args, **kwargs):
        genre_id = self.kwargs.get("genre_id")
        page = request.query_params.get("page", "1")

        logger.debug(f"GenreMoviesView: Fetching movies for genre {genre_id}, page {page}")

        try:
            page = int(page)
            if page < 1:
                logger.warning(f"GenreMoviesView: Invalid page number {page}, defaulting to 1")
                page = 1
        except ValueError:
            logger.warning(f"GenreMoviesView: Invalid page value {page}, defaulting to 1")
            page = 1

        try:
            genre_id = int(genre_id)
        except (ValueError, TypeError):
            logger.warning(f"GenreMoviesView: Invalid genre ID {genre_id}")
            return Response({"detail": "Invalid genre ID"}, status=status.HTTP_400_BAD_REQUEST)

        # Check if genre exists
        try:
            genre = Genre.objects.get(tmdb_id=genre_id)
        except Genre.DoesNotExist:
            logger.warning(f"GenreMoviesView: Genre with ID {genre_id} not found")
            return Response({"detail": "Genre not found"}, status=status.HTTP_404_NOT_FOUND)

        # Try to get from cache
        cache_key = f"genre_movies_{genre_id}_page_{page}"
        cached_data = cache.get(cache_key)

        if cached_data:
            logger.info(f"GenreMoviesView: Retrieved movies for genre {genre_id} from cache")
            return Response(cached_data)

        try:
            # Explicitly pass the page parameter to fetch the correct page from TMDB
            movies, total_pages, total_results = self._get_movies_by_genre(genre_id, page)
            serializer = self.get_serializer(movies, many=True)
            response_data = {
                "results": serializer.data,
                "genre": GenreSerializer(genre).data,
                "page": page,
                "total_pages": total_pages,
                "total_results": total_results,
                "next": None if page >= total_pages else page + 1,
                "previous": None if page <= 1 else page - 1,
                "backdrop_url": movies[0].backdrop_url,
            }

            # Cache the response
            cache.set(cache_key, response_data, 60 * 60)  # 1 hour

            logger.info(
                f"GenreMoviesView: Successfully fetched {len(movies)} movies for genre {genre_id}, page {page} of {total_pages}"
            )
            return Response(response_data)
        except Exception as e:
            logger.error(f"GenreMoviesView: Error fetching movies: {str(e)}", exc_info=True)
            return Response(
                {
                    "results": [],
                    "genre": GenreSerializer(genre).data,
                    "page": page,
                    "total_pages": 0,
                    "total_results": 0,
                    "next": None,
                    "previous": None,
                }
            )

    def _get_movies_by_genre(self, genre_id, page=1):
        """
        Get movies by genre from TMDB API or database
        Returns tuple of (movies, total_pages, total_results)
        """
        logger.debug(f"GenreMoviesView: Fetching movies for genre {genre_id} from TMDB, page {page}")
        try:
            # Get the results from TMDB client
            results = tmdb_client.get_genre_movies(genre_id, page)

            # Get pagination info directly from TMDB client
            try:
                pagination_info = tmdb_client.get_genre_pagination_info(genre_id, page)
                total_pages = pagination_info.get("total_pages", 1)
                total_results = pagination_info.get("total_results", len(results))
            except Exception as e:
                logger.warning(f"GenreMoviesView: Error getting pagination info: {str(e)}. Using defaults.")
                total_pages = 1
                total_results = len(results)

            movies = []
            for result in results:
                logger.debug(f"GenreMoviesView: Processing movie: {result.get('title', 'Unknown')}")
                movie = Movie.objects.create_or_update_from_tmdb(result)
                movies.append(movie)

            return movies, total_pages, total_results
        except Exception as e:
            logger.error(f"GenreMoviesView: Error in _get_movies_by_genre: {str(e)}", exc_info=True)
            raise