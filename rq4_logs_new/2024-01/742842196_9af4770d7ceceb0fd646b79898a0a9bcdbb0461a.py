import aiohttp

from data_validation import DataValidation  # Re-import DataValidation


class APIHandler:
    def __init__(self, gui):
        self.gui = gui
        self.data_validation = DataValidation()

    async def make_async_request(self, query):
        async with aiohttp.ClientSession() as session:
            try:
                url = (
                    f'https://api.example.com/generate?query={query}'
                )
                async with session.get(url) as resp:
                    data = await resp.json()
                    self.data_validation.validate_response(data)  # Validate the response
                    responses = self.parse_response(data)
                    self.gui.display_responses(responses)
            except aiohttp.ClientError as e:
                print(f"Request failed: {e}")
                # Handle the error appropriately
            except ValueError as e:
                print(f"Validation error: {e}")
                # Handle the validation error appropriately

    def parse_response(self, data):
        responses = [item['generated_text'] for item in data['predictions']]
        return responses