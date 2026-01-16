import csv

products = [
    {
      "product_name": "PlayStation 4 500 GB černý",
      "price": 5999,
      "discount": 10,
      "stock": 5,
      "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                     "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                     "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                     "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                     " můžete hrát s přáteli a objevovat nové hry a výzvy.",
      "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
      "visit_count": 8743,
      "product_type": "Herní konzole",
      "brand_id": 9,
      "category_id": 8,
      "color": "černá",
      "product_image": "test_image_ps4.jpeg",
      "ssd_capacity": 0,
      "hdd_capacity": 512,
      "ssd": False,
      "hdd": True,
      "dvd_drive": True,
    },

    {
      "product_name": "PlayStation 4 1000 GB černý",
      "price": 7999,
      "discount": 10,
      "stock": 5,
      "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                     "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                     "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                     "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                     " můžete hrát s přáteli a objevovat nové hry a výzvy.",
      "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
      "visit_count": 18743,
      "product_type": "Herní konzole",
      "brand_id": 9,
      "category_id": 8,
      "color": "černá",
      "product_image": "test_image_ps4.jpeg",
      "ssd_capacity": 0,
      "hdd_capacity": 1024,
      "ssd": False,
      "hdd": True,
      "dvd_drive": True,
    },

    {
      "product_name": "PlayStation Slim 4 500 GB černý",
      "price": 6999,
      "discount": 10,
      "stock": 5,
      "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                     "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                     "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                     "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                     " můžete hrát s přáteli a objevovat nové hry a výzvy.",
      "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
      "visit_count": 8743,
      "product_type": "Herní konzole",
      "brand_id": 9,
      "category_id": 8,
      "color": "černá",
      "product_image": "test_image_ps4.jpeg",
      "ssd_capacity": 0,
      "hdd_capacity": 512,
      "ssd": False,
      "hdd": True,
      "dvd_drive": True,
    },

    {
        "product_name": "PlayStation 4 Slim 1000 GB černý",
        "price": 8999,
        "discount": 10,
        "stock": 5,
        "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                       "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                       "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                       "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                       " můžete hrát s přáteli a objevovat nové hry a výzvy.",
        "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
        "visit_count": 8743,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "černá",
        "product_image": "test_image_ps4.jpeg",
        "ssd_capacity": 0,
        "hdd_capacity": 1024,
        "ssd": False,
        "hdd": True,
        "dvd_drive": True,
    },

    {
        "product_name": "PlayStation 4 500 GB bílý",
        "price": 5999,
        "discount": 10,
        "stock": 5,
        "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                       "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                       "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                       "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                       " můžete hrát s přáteli a objevovat nové hry a výzvy.",
        "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
        "visit_count": 8743,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps4.jpeg",
        "ssd_capacity": 0,
        "hdd_capacity": 512,
        "ssd": False,
        "hdd": True,
        "dvd_drive": True,
    },

    {
        "product_name": "PlayStation 4 1000 GB bílý",
        "price": 7999,
        "discount": 10,
        "stock": 5,
        "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                       "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                       "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                       "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                       " můžete hrát s přáteli a objevovat nové hry a výzvy.",
        "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
        "visit_count": 8743,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps4.jpeg",
        "ssd_capacity": 0,
        "hdd_capacity": 1024,
        "ssd": False,
        "hdd": True,
        "dvd_drive": True,
    },

    {
        "product_name": "PlayStation Slim 4 500 GB bílý",
        "price": 6999,
        "discount": 10,
        "stock": 5,
        "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                       "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                       "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                       "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                       " můžete hrát s přáteli a objevovat nové hry a výzvy.",
        "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
        "visit_count": 8743,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps4.jpeg",
        "ssd_capacity": 0,
        "hdd_capacity": 512,
        "ssd": False,
        "hdd": True,
        "dvd_drive": True,
    },

    {
        "product_name": "PlayStation 4 Slim 1000 GB bílý",
        "price": 8999,
        "discount": 10,
        "stock": 5,
        "description": "PlayStation 4 je herní konzole, která vám přináší neuvěřitelné zážitky ze hraní. "
                       "S výkonným hardwarovým vybavením a širokou nabídkou her si užijete nekonečnou zábavu a "
                       "adrenalinové dobrodružství. Nejnovější technologie vás vtáhnou do světa virtuální reality, "
                       "kde se stanete součástí akce a prožijete neuvěřitelné okamžiky. Navíc díky online připojení"
                       " můžete hrát s přáteli a objevovat nové hry a výzvy.",
        "subheading": "Zažijte herní svět plný zábavy s PlayStation 4.",
        "visit_count": 8743,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps4.jpeg",
        "ssd_capacity": 0,
        "hdd_capacity": 1024,
        "ssd": False,
        "hdd": True,
        "dvd_drive": True,
    },

    {
      "product_name": "PlayStation 5 825 GB s mechanikou",
      "price": 9999,
      "discount": 5,
      "stock": 20,
      "description": "PlayStation 5 je nejnovější generace herní konzole, "
                     "která přináší revoluční zážitky ze hraní. S výkonným hardwarovým vybavením, "
                     "podporou ray tracingu a rychlým načítáním získáte neuvěřitelně realistický a plynulý "
                     "herní prožitek. Díky technologii Tempest 3D AudioTech se ponoříte do zvukového světa, "
                     "který vás vtáhne do hry a poskytne vám dokonalou zvukovou kulisu. "
                     "S novým ovladačem DualSense se hraní stává ještě interaktivnějším díky "
                     "vylepšenému haptickému zpětnému vazbě a adaptivním spouštěm. "
                     "PlayStation 5 je připravena posunout hraní na novou úroveň.",
      "subheading": "Připravte se na neuvěřitelný herní zážitek s PlayStation 5.",
      "visit_count": 5421,
      "product_type": "Herní konzole",
      "brand_id": 9,
      "category_id": 8,
      "color": "bílá",
      "product_image": "test_image_ps5.jpeg",
      "ssd_capacity": 825,
      "hdd_capacity": 0,
      "ssd": True,
      "hdd": False,
      "dvd_drive": True,
    },

    {
        "product_name": "PlayStation 5 1825 GB s mechanikou",
        "price": 10999,
        "discount": 5,
        "stock": 20,
        "description": "PlayStation 5 je nejnovější generace herní konzole, "
                       "která přináší revoluční zážitky ze hraní. S výkonným hardwarovým vybavením, "
                       "podporou ray tracingu a rychlým načítáním získáte neuvěřitelně realistický a plynulý "
                       "herní prožitek. Díky technologii Tempest 3D AudioTech se ponoříte do zvukového světa, "
                       "který vás vtáhne do hry a poskytne vám dokonalou zvukovou kulisu. "
                       "S novým ovladačem DualSense se hraní stává ještě interaktivnějším díky "
                       "vylepšenému haptickému zpětnému vazbě a adaptivním spouštěm. "
                       "PlayStation 5 je připravena posunout hraní na novou úroveň.",
        "subheading": "Připravte se na neuvěřitelný herní zážitek s PlayStation 5.",
        "visit_count": 5421,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps5.jpeg",
        "ssd_capacity": 1825,
        "hdd_capacity": 0,
        "ssd": True,
        "hdd": False,
        "dvd_drive": True,
    },

    {
        "product_name": "PlayStation 5 2825 GB s mechanikou",
        "price": 12999,
        "discount": 5,
        "stock": 20,
        "description": "PlayStation 5 je nejnovější generace herní konzole, "
                       "která přináší revoluční zážitky ze hraní. S výkonným hardwarovým vybavením, "
                       "podporou ray tracingu a rychlým načítáním získáte neuvěřitelně realistický a plynulý "
                       "herní prožitek. Díky technologii Tempest 3D AudioTech se ponoříte do zvukového světa, "
                       "který vás vtáhne do hry a poskytne vám dokonalou zvukovou kulisu. "
                       "S novým ovladačem DualSense se hraní stává ještě interaktivnějším díky "
                       "vylepšenému haptickému zpětnému vazbě a adaptivním spouštěm. "
                       "PlayStation 5 je připravena posunout hraní na novou úroveň.",
        "subheading": "Připravte se na neuvěřitelný herní zážitek s PlayStation 5.",
        "visit_count": 5421,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps5.jpeg",
        "ssd_capacity": 2825,
        "hdd_capacity": 0,
        "ssd": True,
        "hdd": False,
        "dvd_drive": True,
    },

    {
        "product_name": "PlayStation 5 825 GB bez mechaniky",
        "price": 8999,
        "discount": 5,
        "stock": 20,
        "description": "PlayStation 5 je nejnovější generace herní konzole, "
                       "která přináší revoluční zážitky ze hraní. S výkonným hardwarovým vybavením, "
                       "podporou ray tracingu a rychlým načítáním získáte neuvěřitelně realistický a plynulý "
                       "herní prožitek. Díky technologii Tempest 3D AudioTech se ponoříte do zvukového světa, "
                       "který vás vtáhne do hry a poskytne vám dokonalou zvukovou kulisu. "
                       "S novým ovladačem DualSense se hraní stává ještě interaktivnějším díky "
                       "vylepšenému haptickému zpětnému vazbě a adaptivním spouštěm. "
                       "PlayStation 5 je připravena posunout hraní na novou úroveň.",
        "subheading": "Připravte se na neuvěřitelný herní zážitek s PlayStation 5.",
        "visit_count": 5421,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps5.jpeg",
        "ssd_capacity": 825,
        "hdd_capacity": 0,
        "ssd": True,
        "hdd": False,
        "dvd_drive": False,
    },

    {
        "product_name": "PlayStation 5 1825 GB bez mechaniky",
        "price": 9999,
        "discount": 5,
        "stock": 20,
        "description": "PlayStation 5 je nejnovější generace herní konzole, "
                       "která přináší revoluční zážitky ze hraní. S výkonným hardwarovým vybavením, "
                       "podporou ray tracingu a rychlým načítáním získáte neuvěřitelně realistický a plynulý "
                       "herní prožitek. Díky technologii Tempest 3D AudioTech se ponoříte do zvukového světa, "
                       "který vás vtáhne do hry a poskytne vám dokonalou zvukovou kulisu. "
                       "S novým ovladačem DualSense se hraní stává ještě interaktivnějším díky "
                       "vylepšenému haptickému zpětnému vazbě a adaptivním spouštěm. "
                       "PlayStation 5 je připravena posunout hraní na novou úroveň.",
        "subheading": "Připravte se na neuvěřitelný herní zážitek s PlayStation 5.",
        "visit_count": 5421,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps5.jpeg",
        "ssd_capacity": 1825,
        "hdd_capacity": 0,
        "ssd": True,
        "hdd": False,
        "dvd_drive": False,
    },

    {
        "product_name": "PlayStation 5 2825 GB bez mechaniky",
        "price": 11999,
        "discount": 5,
        "stock": 20,
        "description": "PlayStation 5 je nejnovější generace herní konzole, "
                       "která přináší revoluční zážitky ze hraní. S výkonným hardwarovým vybavením, "
                       "podporou ray tracingu a rychlým načítáním získáte neuvěřitelně realistický a plynulý "
                       "herní prožitek. Díky technologii Tempest 3D AudioTech se ponoříte do zvukového světa, "
                       "který vás vtáhne do hry a poskytne vám dokonalou zvukovou kulisu. "
                       "S novým ovladačem DualSense se hraní stává ještě interaktivnějším díky "
                       "vylepšenému haptickému zpětnému vazbě a adaptivním spouštěm. "
                       "PlayStation 5 je připravena posunout hraní na novou úroveň.",
        "subheading": "Připravte se na neuvěřitelný herní zážitek s PlayStation 5.",
        "visit_count": 5421,
        "product_type": "Herní konzole",
        "brand_id": 9,
        "category_id": 8,
        "color": "bílá",
        "product_image": "test_image_ps5.jpeg",
        "ssd_capacity": 2825,
        "hdd_capacity": 0,
        "ssd": True,
        "hdd": False,
        "dvd_drive": False,
    },

    {
      "product_name": "Xbox Series X 100GB",
      "price": 13999,
      "discount": 5,
      "stock": 20,
      "description": "Xbox Series X je nejnovější generace herní konzole, která vám přináší úžasný herní zážitek. "
                     "S výkonným hardwarovým vybavením a podporou ray tracingu si užijete realistické a plynulé hraní"
                     " s nádhernou grafikou. Díky rychlému načítání se okamžitě dostanete do hry a můžete si "
                     "užívat bez prodlev. Xbox Series X je vybavený technologií Spatial Sound, která vám poskytuje "
                     "výjimečný prostorový zvuk a ponoří vás přímo do děje hry. S novým bezdrátovým ovladačem si "
                     "užijete přesnou a pohodlnou kontrolu nad hrou. Xbox Series X je připravený rozšířit"
                     " vaše hraní na novou úroveň.",
      "subheading": "Připravte se na úžasný herní zážitek s Xbox Series X.",
      "visit_count": 5421,
      "product_type": "Herní konzole",
      "brand_id": 10,
      "category_id": 8,
      "color": "černá",
      "product_image": "test_image_xbox_series_x.jpeg",
      "ssd_capacity": 1024,
      "hdd_capacity": 0,
      "ssd": True,
      "hdd": False,
      "dvd_drive": True
    },

    {
      "product_name": "Xbox Series S 500GB",
      "price": 6999,
      "discount": 0,
      "stock": 15,
      "description": "Xbox Series S je kompaktní a výkonná herní konzole, která vám přináší skvělý herní zážitek. "
                     "S výkonným hardwarovým vybavením a podporou ray tracingu si užijete plynulé a detailní "
                     "hraní ve vysokém rozlišení. Díky rychlému načítání se okamžitě dostanete do hry a můžete "
                     "si užívat nekonečné možnosti zábavy. Xbox Series S je navržena tak, aby byla kompaktní a "
                     "snadno přenosná, což z ní činí skvělou volbu pro hráče, kteří chtějí hrát doma i na cestách. "
                     "S novým bezdrátovým ovladačem si užijete precizní a pohodlnou kontrolu nad hrou. Xbox Series "
                     "S je připravená rozšířit vaše hraní na novou úroveň.",
      "subheading": "Připravte se na skvělý herní zážitek s Xbox Series S.",
      "visit_count": 3762,
      "product_type": "Herní konzole",
      "brand_id": 10,
      "category_id": 8,
      "color": "bílá",
      "product_image": "test_image_xbox_series_s.jpeg",
      "ssd_capacity": 512,
      "hdd_capacity": 0,
      "ssd": True,
      "hdd": False,
      "dvd_drive": False,
    }
]


with open('console_products.csv', 'w', newline='') as file:
    # create a writer object
    writer = csv.writer(file)

    # write the header row
    writer.writerow(products[0].keys())

    # write the data rows
    for product in products:
        writer.writerow(product.values())