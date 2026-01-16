import unittest
from unittest.mock import patch, MagicMock
from todo_cli import fetch_todo, fetch_even_todos, display_todos

class TestTodoCLI(unittest.TestCase):

    @patch('todo_cli.requests.get')
    def test_fetch_todo(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {'id': 1, 'title': 'Test Todo', 'completed': False}
        mock_get.return_value = mock_response
        todo = fetch_todo(1)
        self.assertEqual(todo['id'], 1)
        self.assertEqual(todo['title'], 'Test Todo')
        self.assertEqual(todo['completed'], False)

    @patch('todo_cli.fetch_todo')
    def test_fetch_even_todos(self, mock_fetch_todo):
        mock_fetch_todo.side_effect = lambda x: {'id': x, 'title': f'Todo {x}', 'completed': False}
        todos = fetch_even_todos()
        self.assertEqual(len(todos), 20)
        for i, todo in enumerate(todos):
            self.assertEqual(todo['id'], 2*i + 2)
            self.assertEqual(todo['title'], f'Todo {2*i + 2}')
            self.assertEqual(todo['completed'], False)

    @patch('builtins.print')
    def test_display_todos(self, mock_print):
        todos = [
            {'title': 'Todo 1', 'completed': False},
            {'title': 'Todo 2', 'completed': True}
        ]
        display_todos(todos)
        mock_print.assert_any_call('Title: Todo 1 - Completed: No')
        mock_print.assert_any_call('Title: Todo 2 - Completed: Yes')

if __name__ == '__main__':
    unittest.main()