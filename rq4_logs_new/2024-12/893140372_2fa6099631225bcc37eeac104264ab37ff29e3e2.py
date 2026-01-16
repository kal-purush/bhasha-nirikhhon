import unittest
import numpy as np
from main import join_viewing_history_with_netflix

class TestDataProcessingEnrichment(unittest.TestCase):
    def test_join_viewing_history_with_netflix_normal(self):
        """
        Test normal joining of viewing history with Netflix show data.
        """
        reduced_history = np.array([
            ["The Blacklist", "52"],
            ["Breaking Bad", "12"],
        ])

        netflix_show_data = np.array([
            ["s1", "TV Show", "The Blacklist", "Crime/Drama"],
            ["s2", "TV Show", "Breaking Bad", "Crime/Thriller"],
        ])

        result = join_viewing_history_with_netflix(reduced_history, netflix_show_data)

        expected = np.array([
            ["The Blacklist", "52", "s1", "TV Show", "The Blacklist", "Crime/Drama"],
            ["Breaking Bad", "12", "s2", "TV Show", "Breaking Bad", "Crime/Thriller"],
        ])

        np.testing.assert_array_equal(result, expected)

    def test_join_viewing_history_with_netflix_missing_title(self):
        """
        Test when some titles in viewing history are not in Netflix show data.
        """
        reduced_history = np.array([
            ["The Blacklist", "52"],
            ["Unknown Show", "10"],
        ])

        netflix_show_data = np.array([
            ["s1", "TV Show", "The Blacklist", "Crime/Drama"],
        ])

        result = join_viewing_history_with_netflix(reduced_history, netflix_show_data)

        expected = np.array([
            ["The Blacklist", "52", "s1", "TV Show", "The Blacklist", "Crime/Drama"],
        ])  # "Unknown Show" is not included

        np.testing.assert_array_equal(result, expected)

    def test_join_viewing_history_with_netflix_empty_data(self):
        """
        TODO - Test behavior when either viewing history or Netflix show data is empty.
        """
        return

    def test_join_viewing_history_with_netflix_partial_match(self):
        """
        Test when some titles partially match in Netflix show data.
        """
        reduced_history = np.array([
            ["Blacklist", "52"],
            ["Breaking Bad", "12"],
        ])

        netflix_show_data = np.array([
            ["s1", "TV Show", "The Blacklist", "Crime/Drama"],
            ["s2", "TV Show", "Breaking Bad", "Crime/Thriller"],
        ])

        result = join_viewing_history_with_netflix(reduced_history, netflix_show_data)

        expected = np.array([
            ["Breaking Bad", "12", "s2", "TV Show", "Breaking Bad", "Crime/Thriller"],
        ])  # "Blacklist" does not fully match "The Blacklist"

        np.testing.assert_array_equal(result, expected)

    def test_join_viewing_history_with_netflix_duplicate_titles(self):
        """
        Test behavior when titles in viewing history or Netflix show data are duplicated.
        """
        reduced_history = np.array([
            ["The Blacklist", "52"],
            ["The Blacklist", "53"],
        ])

        netflix_show_data = np.array([
            ["s1", "TV Show", "The Blacklist", "Crime/Drama"],
            ["s2", "TV Show", "The Blacklist", "Crime/Drama"],  # Duplicate entry
        ])

        result = join_viewing_history_with_netflix(reduced_history, netflix_show_data)

        expected = np.array([
            ["The Blacklist", "52", "s2", "TV Show", "The Blacklist", "Crime/Drama"],
            ["The Blacklist", "53", "s2", "TV Show", "The Blacklist", "Crime/Drama"],
        ])  # Last match used, duplicate ignored

        np.testing.assert_array_equal(result, expected)


if __name__ == "__main__":
    unittest.main()