from odoo import api, fields, models
class StockPicking(models.Model):
    _inherit = 'stock.picking'

    def export_vex_product(self):
        products = []
        for line in self.move_line_ids_without_package:
            if line.product_id:
                if line.product_id.id not in products:
                    products.append(line.product_id.id)

        productss = self.env['product.product'].search([('id','in',products)])
        productss.update_conector_vex()


    def button_validate(self):
        res = super().button_validate()
        self.export_vex_product(False)
        return res