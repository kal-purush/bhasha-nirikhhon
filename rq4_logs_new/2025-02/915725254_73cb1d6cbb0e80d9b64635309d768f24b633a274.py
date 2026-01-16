from abc import abstractmethod


class AbstractEmailSender:
    @abstractmethod
    def send(self, to: str, message: str): ...