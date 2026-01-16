""" Libor Havránek App Copyright (C)  12.5 2023 """

from flask import flash
from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed
from wtforms import StringField, SubmitField, TextAreaField, SelectField, DecimalField, IntegerField, HiddenField, \
    FileField, MultipleFileField, BooleanField
from wtforms.validators import InputRequired, Length, Optional, NumberRange, ValidationError

from myshop.models.brand_model import Brand
from myshop.models.category_model import Category
from myshop.models.product_model import Product


class MonitorForm(FlaskForm):
    csrf_token = HiddenField()
    product_name = StringField('Název *:', validators=[InputRequired(), Length(max=80)])
    price = DecimalField('Cena *:', validators=[InputRequired(), NumberRange(min=0, max=999999.99)], default=0)
    discount = IntegerField('Sleva:', validators=[NumberRange(min=0, max=100)], default=0)
    stock = IntegerField('Počet kusů:', validators=[NumberRange(min=0)], default=0)
    description = TextAreaField('Popis *:', validators=[InputRequired()], render_kw={'rows': 10})
    subheading = TextAreaField('Podnadpis *:', validators=[InputRequired()])
    display_size = DecimalField('Velikost displeje *:', validators=[InputRequired(), NumberRange(min=0)], default=0)
    display_resolution = SelectField('Rozlišení displeje *:', validators=[Optional()],
                                     choices=[('1920x1080', '1920 × 1080'), ('2560x1440', '2560 × 1440'),
                                              ('3840x2160', '3840 × 2160'), ('5120x2880', '5120 × 2880')])  # Add your choices here
    refresh_rate = IntegerField('Obnovovací frekvence *:', validators=[InputRequired(), NumberRange(min=0)], default=0)
    response_time = IntegerField('Doba odezvy *:', validators=[InputRequired(), NumberRange(min=0)], default=0)
    aspect_ratio = SelectField('Poměr stran *:', validators=[InputRequired()],
                               choices=[('16:9', '16:9'), ('16:10', '16:10'), ('21:9', '21:9')])  # Add your choices here
    connectivity = StringField('Konektivita *:', validators=[InputRequired()])
    color_depth = IntegerField('Hloubka barev:', validators=[NumberRange(min=0)], default=0)
    curvature = BooleanField('Zakřivený displej:')
    height = DecimalField('Výška:', validators=[NumberRange(min=0, max=999999.99)], default=0)
    height_units = SelectField('Velikostní jednotka:', choices=[('cm', 'cm'), ('in', 'in'), ('mm', 'mm')], validators=[Optional()])
    width = DecimalField('Šířka:', validators=[NumberRange(min=0, max=999999.99)], default=0)
    width_units = SelectField('Velikostní jednotka:', choices=[('cm', 'cm'), ('in', 'in'), ('mm', 'mm')], validators=[Optional()])
    depth = DecimalField('Hloubka:', validators=[NumberRange(min=0, max=999999.99)], default=0)
    depth_units = SelectField('Velikostní jednotka:', choices=[('cm', 'cm'), ('in', 'in'), ('mm', 'mm')], validators=[Optional()])
    weight = DecimalField('Váha:', validators=[NumberRange(min=0, max=999999.99)], default=0)
    weight_units = SelectField('Váhová jednotka:', choices=[('g', 'g'), ('kg', 'kg')], validators=[Optional()])
    brand_id = SelectField('Značka:', coerce=int, choices=[], validators=[Optional()])

    category_id = SelectField('Kategorie:', coerce=int, choices=[], validators=[Optional()])

    product_image = FileField('Foto *:', validators=[InputRequired(), FileAllowed(['jpg', 'png', 'jpeg', 'webp'], 'Images only!')])
    additional_images = MultipleFileField('Další fotky:', validators=[FileAllowed(['jpg', 'png', 'jpeg', 'webp'], 'Images only!')])

    add_product_submit = SubmitField('Přidat produkt')
    edit_product_submit = SubmitField('Upravit produkt')

    def __init__(self, *args, **kwargs):
        super(MonitorForm, self).__init__(*args, **kwargs)
        self.brand_id.choices = [(brand.id, brand.brand_name) for brand in Brand.query.all()]
        self.category_id.choices = [(category.id, category.category_name) for category in Category.query.all()]

    def validate_product_name(self, product_name):
        if len(product_name.data.strip()) < 2:
            flash('Produkt musí mít alespoň dva znaky.', 'error')
            raise ValidationError('Produkt musí mít alespoň dva znaky.')

        product = Product.query.filter_by(product_name=product_name.data).first()
        if product:
            flash('Tento produkt je už zaregistrován v naší databázi.', 'error')
            raise ValidationError('Tento produkt je už zaregistrován v naší databázi.')

    def validate_subheading(self, subheading):
        if len(subheading.data.strip()) < 20:
            flash('Podnadpis musí mít alespoň dvacet znaků.', 'error')
            raise ValidationError('Podnadpis musí mít alespoň dvacet znaků.')

    def validate_description(self, subheading):
        if len(subheading.data.strip()) < 50:
            flash('Popis musí mít alespoň padesát znaků.', 'error')
            raise ValidationError('Popis musí mít alespoň padesát znaků.')

    def validate_price(self, price):
        if price.data == 0:
            flash('Cena produktu nemůže být nulová.', 'error')
            raise ValidationError('Cena produktu nemůže být nulová.', 'error')