interface Product {
  Product: {
    active: boolean;
    brand: string;
    client_id: string;
    code: number;
    created_at: string;
    created_by: string;
    creator_type: string;
    description: string;
    franchise_id: string;
    id: string;
    image: string;
    is_additional: boolean;
    is_deleted: boolean;
    name: string;
    product_consumption_unit_id: string;
    promoter_id: string;
    ref_id: string;
    type: string;
    updated_at: string;
  };
  active: boolean;
  consumption_quantity: string;
  created_at: string;
  is_deleted: boolean;
  menu_item_id: string;
  product_id: string;
  updated_at: string;
}

interface PubMenu {
  Pub: {
    Event: {
      active: boolean;
      address: string;
      category: string;
      city: string;
      client_id: string;
      complement: string;
      created_at: string;
      currency_type: string;
      franchise_id: string;
      id: string;
      is_deleted: boolean;
      latitude: string;
      location: string;
      longitude: string;
      name: string;
      neighborhood: string;
      number: string;
      promoter_id: string;
      recurrence_type: string;
      ref_id: string;
      reveal_location_after: boolean;
      state: string;
      subject: string;
      synced: boolean;
      time_zone_id: string;
      type: string;
      updated_at: string;
      visibility: string;
      zipcode: string;
    };
    accept_cashless: boolean;
    accept_command: boolean;
    active: boolean;
    add_waiter_commission: boolean;
    allow_redeem_balance: boolean;
    cashless_card_cost: string;
    client_tax: string;
    created_at: string;
    event_id: string;
    first_footer_line: string;
    id: string;
    logo_image: string;
    payment_type: string;
    production_password_init: number;
    promoter_tax: string;
    redeem_tax: string;
    second_footer_line: string;
    third_footer_line: string;
    updated_at: string;
  };
  active: boolean;
  created_at: string;
  is_deleted: boolean;
  menu_id: string;
  pub_id: string;
  updated_at: string;
}

export interface MenuById {
  Group: {
    active: boolean;
    created_at: string;
    id: string;
    is_deleted: boolean;
    logo_image: string;
    menu_id: string;
    name: string;
    order: boolean;
    updated_at: string;
  }[];
  Itens: {
    ExtraItem: [];
    ExtraProduct: [];
    Group: {
      active: boolean;
      created_at: string;
      id: string;
      is_deleted: boolean;
      logo_image: string;
      menu_id: string;
      name: string;
      order: number;
      updated_at: string;
    };
    Product: Product[];
    PebMenu: PubMenu[];
    Promoter: {
      active: boolean;
      created_at: string;
      document: string;
      id: string;
      name: string;
      updated_at: string;
      use_products_database: boolean;
    };
    active: boolean;
    client_id: string;
    created_at: string;
    franchise_id: string;
    id: string;
    is_deleted: boolean;
    name: string;
    promoter_id: string;
    updated_at: string;

    sale_price: string;
  }[];
}