import pytest
from book_extractors.karelians.extraction.extractors.profession_extractor import ProfessionExtractor


class TestProfessionExtraction:

    @pytest.yield_fixture(autouse=True)
    def profession_extractor(self):
        return ProfessionExtractor(None, None)

    def should_add_extra_info_to_profession_if_it_is_available(self, profession_extractor):
        text = 'maanviljelijä, synt. 18. 6. -29 Hiitolassa. Puol. Vaimo Vaimokas'

        results, metadata = profession_extractor.extract({'text': text}, {}, {})

        extra_info = results['profession']['extraInfo']
        assert extra_info is not None
        assert results['profession']['professionName'] == 'maanviljelijä'
        assert extra_info == {
            'manualLabor': True,
            'englishName': 'farmer (owns farm)',
            'education': True,
            'SESgroup1989': 1,
            'agricultureOrForestryRelated': True,
            'occupationCategory': 3,
            'socialClassRank': 5

        }

    def should_set_extra_info_null_if_no_extra_data_is_available(self, profession_extractor):
        text = 'koirankynnenleikkaaja, synt. 18. 6. -29 Hiitolassa. Puol. Vaimo Vaimokas'

        results, metadata = profession_extractor.extract({'text': text}, {}, {})
        assert results['profession']['extraInfo'] is None
        assert results['profession']['professionName'] == 'koirankynnenleikkaaja'