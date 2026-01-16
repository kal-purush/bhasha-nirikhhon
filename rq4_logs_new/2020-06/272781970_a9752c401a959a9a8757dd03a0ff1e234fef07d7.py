from phones.models import Phone
import csv


class Imp:
    filename = 'phones.csv'

    def handle(self, *args, **options):
        if args:
            self.filename = args[0]

        with open(file=self.filename) as f:
            lines = csv.DictReader(f, delimiter=';')
            for i in lines:
                Phone.objects.create(name=i.get('name'),
                                     price=i.get('price'),
                                     image=i.get('image'),
                                     release_date=i.get('release_date'),
                                     lte_exists=i.get('lte_exists'),
                                     )