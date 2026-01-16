import unittest
from unittest.mock import patch, call, Mock, MagicMock

from app.black_jack.black_jack import BlackJack
from app.dealer.dealer import Dealer
from app.player.player import Player


class TestBlackJack(unittest.TestCase):

    @patch('app.black_jack.black_jack.print')
    def test_if_print_has_been_called(self, print_mock):
        player_mock = MagicMock(return_value='João')
        dealer_mock = MagicMock(return_value='Guto')
        player_mock.get_name = player_mock
        black_jack = BlackJack(player_mock, dealer_mock)
        black_jack.welcome_message()
        assert print_mock.mock_calls == [call("=" * 60),
                                         call(f"Seja bem vindo ao Black Jack João !!!"),
                                         call("=" * 60)]
        player_mock.get_name.assert_called_once()
        print_mock.mock_called()

    @patch.object(BlackJack, 'show_retired_card')
    @patch.object(BlackJack, 'addict_score_player')
    def test_dealer_delivering_card(self, addict_score_player_mock, show_retired_card_mock):
        dealer_mock = MagicMock(return_value="dealer")
        player_mock = MagicMock(return_value='João')
        dealer_mock.get_card_from_deck = MagicMock(return_value='1 de Paus')
        addict_score_player_mock(dealer_mock.get_card_from_deck)
        player_mock.receive_card = dealer_mock.get_card_from_deck
        dealer_mock.remove_card_from_deck = dealer_mock.get_card_from_deck
        show_retired_card_mock(dealer_mock.get_card_from_deck)
        black_jack = BlackJack(player_mock, dealer_mock)
        black_jack.dealer_delivering_card()
        dealer_mock.get_card_from_deck.assert_called()
        assert dealer_mock.get_card_from_deck.mock_calls == [call(), call('1 de Paus'), call('1 de Paus')]
        assert dealer_mock.get_card_from_deck.mock_called
        assert addict_score_player_mock.called

    @patch('app.black_jack.black_jack.Card')
    def test_addict_score_player(self, card_mock):
        dealer_mock = MagicMock(return_value="dealer")
        player_mock = MagicMock(return_value='João')
        player_mock.score = MagicMock(return_value=10)
        # card_mock = MagicMock(Mock())
        card_mock = MagicMock(return_value='Q')
        black_jack = BlackJack(player_mock, dealer_mock)
        black_jack.addict_score_player(card_mock)
        assert card_mock.mock_calls == [call.get_name(),
                                         call.get_name().__eq__('Q'),
                                         call.get_name(),
                                         call.get_name().__eq__('J'),
                                         call.get_value(),
                                         call.get_value().__eq__('K'),
                                         call.get_value()]






