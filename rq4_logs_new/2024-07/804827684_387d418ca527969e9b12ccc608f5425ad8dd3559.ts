import { AbstractNotificationService } from "@medusajs/medusa";
import nodemailer from "nodemailer";
import { EntityManager } from "typeorm";

class EmailSenderService extends AbstractNotificationService {
  static identifier = "email-sender";

  protected manager_: EntityManager;
  protected transactionManager_: EntityManager;

  orderService;
  cartService;
  inviteService;
  swapService;
  transporter;
  transportConf;
  constructor(container, config) {
    super(container);
    this.transportConf = {
      host: process.env.MAILBOX_H,
      port: 465,
      secure: true,
      auth: {
        user: process.env.MAILBOX_USER,
        pass: process.env.MAILBOX_PASS,
      },
    };

    this.transporter = nodemailer.createTransport(this.transportConf);

    this.orderService = container.orderService;
    this.cartService = container.cartService;
    this.inviteService = container.inviteService;
    this.swapService = container.swapService;
  }

  async sendNotification(
    event: string,
    data,
    attachmentGenerator: unknown
  ): Promise<{
    to: string;
    status: string;
    data: Record<string, unknown>;
  }> {
    if (event === "order.placed") {
      // retrieve order
      const order = await this.orderService.retrieve(data.id, {
        select: ["shipping_total", "tax_total", "subtotal", "total"],
        relations: [
          "customer",
          "billing_address",
          "shipping_address",
          "discounts",
          "discounts.rule",
          "shipping_methods",
          "shipping_methods.shipping_option",
          "payments",
          "fulfillments",
          "fulfillments.tracking_links",
          "returns",
          "gift_cards",
          "gift_card_transactions",
          "items",
        ],
      });
      // TODO send email
      const message = {
        from: "Магазин болагро <vatsikmax@ukr.net>",
        to: order.email,
        subject: "Деталі замовлення, магазин Болагро",
        html: `<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Деталі замовлення</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
        pre {
            background: #f4f4f4;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 5px;
            overflow: auto;
        }
        h2 {
            color: #333;
        }
    </style>
</head>
<body>
    <h1>Деталі замовлення</h1>
    <div id="custom-explanation"></div>
            <p><strong>Дата:</strong> ${new Date(
              order.created_at
            ).toLocaleString()}</p>
            <h3>Товари: </h3>
    <table id="itemsTable">
        <thead>
            <tr>
                <th>Назва</th>
                <th>Ціна (${order.currency_code})</th>
                <th>Кількість</th>
                <th>Загалом (${order.currency_code})</th>
                ${order.items
                  .map(
                    (item) => `
            <tr>
                <td>${item.variant.product.title} (${item.variant.title})</td>
                <td>${item.unit_price}</td>
                <td>${item.quantity}</td>
                <td>${item.total}</td>
            </tr>
        `
                  )
                  .join("")}
            </tr>
        </thead>
        <tbody></tbody>
    </table>            
            <h3>Адреса доставки:</h3>
            <p>
                ${order.shipping_address.address_1} ${
          order.shipping_address.address_2
        }
                ${order.shipping_address.city}<br>
                Телефон: ${order.shipping_address.phone}
            </p>
                ${order.shipping_address.first_name} ${
          order.shipping_address.last_name
        }            
            <p>

            </p>
            <h3>Сервіс доставки:</h3>
            <p>${order.shipping_methods[0].shipping_option.name}</p>
            <h3>До оплати:</h3>

    <table id="priceTable">
        <thead>
            <tr>
                <th>Доставка (${order.currency_code})</th>
                <th>Ціна товарів (${order.currency_code})</th>
                <th>Загалом (${order.currency_code})</th>
            <tr>
                <td>${order.shipping_total}</td>
                <td>${order.subtotal}</td>
                <td>${order.total}</td>
            </tr>
        </thead>
        <tbody></tbody>
    </table>            
            <h3>Дякуємо за замовлення, ми незабаром зв'яжемося з вами</h3>
</body>
</html>`,
      };
      const sendMailToClientPromise = this.transporter.sendMail(message);
      const adminMessage = {
        ...message,
        to: this.transportConf.auth.user,
      };
      console.log(adminMessage);
      const sendMailToShopPromise = this.transporter.sendMail(adminMessage);
      await Promise.all([sendMailToClientPromise, sendMailToShopPromise]);

      return {
        to: order.email,
        status: "done",
        data: {
          subject: "You placed a new order!",
          items: order.items,
        },
      };
    }
  }

  async resendNotification(
    notification: unknown,
    config: unknown,
    attachmentGenerator: unknown
  ): Promise<{
    to: string;
    status: string;
    data: Record<string, unknown>;
  }> {
    throw new Error("Method not implemented.");
  }
}

export default EmailSenderService;