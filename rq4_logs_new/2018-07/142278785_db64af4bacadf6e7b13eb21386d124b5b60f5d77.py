from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone


from ..models import Eigenschap

from .factories import (
    EigenschapFactory, EigenschapReferentieFactory, EigenschapSpecificatieFactory,
    ZaakTypeFactory
)


class EigenschapModelTests(TestCase):
    def test_model_raises_error_when_both_specificatie_and_referentie_are_set(self):
        zaaktype = ZaakTypeFactory.create()
        specificatie = EigenschapSpecificatieFactory.create()
        referentie = EigenschapReferentieFactory.create()

        eigenschap = EigenschapFactory.create(
            definitie="definitie",
            eigenschapnaam="aard product",
            toelichting="nieuw / verandering / ambtshalve wijziging / ontheffing / intrekking",
            specificatie_van_eigenschap=specificatie,
            referentie_naar_eigenschap=referentie,
            is_van=zaaktype
        )

        self.assertRaises(ValidationError, eigenschap.clean)

    def test_model_raises_error_when_both_fields_are_not_set(self):
        zaaktype = ZaakTypeFactory.create()

        eigenschap = EigenschapFactory.create(
            definitie="definitie",
            eigenschapnaam="aard product",
            toelichting="nieuw / verandering / ambtshalve wijziging / ontheffing / intrekking",
            is_van=zaaktype
        )

        self.assertRaises(ValidationError, eigenschap.clean)

    def test_model_does_not_raise_an_error_when_only_specificatie_is_set(self):
        zaaktype = ZaakTypeFactory.create()
        specificatie = EigenschapSpecificatieFactory.create()

        eigenschap = EigenschapFactory.create(
            definitie="definitie",
            eigenschapnaam="aard product",
            toelichting="nieuw / verandering / ambtshalve wijziging / ontheffing / intrekking",
            specificatie_van_eigenschap=specificatie,
            is_van=zaaktype
        )

        self.assertIsNone(eigenschap.clean())

    def test_model_does_not_raise_an_error_when_only_referentie_is_set(self):
        zaaktype = ZaakTypeFactory.create()
        referentie = EigenschapReferentieFactory.create()

        eigenschap = EigenschapFactory.create(
            definitie="definitie",
            eigenschapnaam="aard product",
            toelichting="nieuw / verandering / ambtshalve wijziging / ontheffing / intrekking",
            referentie_naar_eigenschap=referentie,
            is_van=zaaktype
        )

        self.assertIsNone(eigenschap.clean())