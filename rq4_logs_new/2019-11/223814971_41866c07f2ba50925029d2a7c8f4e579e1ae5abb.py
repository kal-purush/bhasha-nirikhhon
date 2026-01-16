import folium

# mapa de ribeirão pires
m = folium.Map(location = [-23.713640, -46.413690], zoom_start=15)

# <--- OUTROS CENTROS DE COMPRA E VENDA DE SUCATAS INICIO--->
# RIBPEL
# add restante dos parametros
tooltip_RIBPEL='RECEBE DIVERSOS MATERIAS'
folium.Marker([-23.708087, -46.412085], popup='<strong>RibPel</strong>',
                                        tooltip=tooltip_RIBPEL,
                                        icon=folium.Icon(color='green', prefix='fa', icon='fas fa-recycle')).add_to(m)

# COOPERPIRES
tooltip_COOPERPIRES='CENTRO DE COLETA, RECEBE DIVERSOS MATERIAS'
folium.Marker([-23.690552, -46.429625], popup='<strong>RibPel</strong>',
                                        tooltip=tooltip_COOPERPIRES,
                                        icon=folium.Icon(color='green', prefix='fa', icon='fas fa-recycle')).add_to(m)

# SUCATAS WILSON RABETTI
tooltip_W="RECEBE DIVERSOS MATERIAS"
folium.Marker([23.694934, -46.432773],  popup='<strong>SWR SUCATAS</strong>',
                                        tooltip=tooltip_W,
                                        icon=folium.Icon(color='green', prefix='fa', icon='fas fa-recycle')).add_to(m)
                                       
# FERRO VELHO STª LUZIA
tooltip_STA_LUZIA='RECEBE DIVERSOS MATERIAS'
folium.Marker([-23.694712, -46.395653], popup='<strong>Ferro Velho Stª Luzia</strong>',
                                        tooltip=tooltip_STA_LUZIA,
                                        icon=folium.Icon(color='green', prefix='fa', icon='fas fa-recycle')).add_to(m)

# DISK SUCATAS
tooltip_DISK='RECEBE DIVERSOS MATERIAS'
folium.Marker([-23.701176, -46.390745], popup='<strong>Disk Sucatasa <br>Jardim Itapevi</strong>',
                                        tooltip=tooltip_DISK,
                                        icon=folium.Icon(color='green', prefix='fa', icon='fas fa-recycle')).add_to(m)

# Mm Comercio de Sucatas 
tooltip_MM='RECEBE DIVERSOS MATERIAS'
folium.Marker([-23.694775, -46.435647], popup='<strong>MM Comércio de Sucatas</strong>',
                                        tooltip=tooltip_MM,
                                        icon=folium.Icon(color='green', prefix='fa', icon='fas fa-recycle')).add_to(m)

# SUL FERRO COMERCIO DE METAIS
tooltip_SUL='COMPRA E REVENDE RETALHOS E PLACAS DE FERRO, ALUMÍNIO E METAIS DIVERSOS'
folium.Marker([-23.701187, -46.391783], popup='<strong>Sul Ferro Comércio de Metais \,,/.</strong>',
                                        tooltip=tooltip_SUL,
                                        icon=folium.Icon(color='green', prefix='fa', icon='fas fa-recycle')).add_to(m)

# <--- OUTROS CENTROS DE COMPRA E VENDA DE SUCATAS FIM--->

# <--- PILHAS E BATERIAS INICIO--->
# COMPRE BEM FCO MONT
tooltip_FCO_MONT='RECEBE: PILHAS E BATERIAS'
folium.Marker([-23.712342, -46.411989], popup='<strong>Compre Bem</strong>',
                                        tooltip=tooltip_FCO_MONT,
                                        icon=folium.Icon(color='orange', icon='flash')).add_to(m)

# ASSAÍ ATACADISTA
tooltip_ASSAI='RECEBE: PILHAS E BATERIAS'
folium.Marker([-23.703474, -46.397564], popup='<strong>Assaí Atacadista</strong>',
                                        toooltip=tooltip_ASSAI,
                                        icon=folium.Icon(color='orange', icon='flash')).add_to(m) 

# COOP CENTRO ALTO
tooltip_COOP_C='RECEBE: PILHAS E BATERIAS'
folium.Marker([-23.716758, -46.412186], popup='<strong>COOP Centro Alto</strong>',
                                        tooltip=tooltip_COOP_C,
                                        icon=folium.Icon(color='orange', icon='flash')).add_to(m)

# COOP HUMBERTO CAMPOS
tooltip_COOP_HCAMPOS='RECEBE: PILHAS E BATERIAS'
folium.Marker([-23.696868, -46.444356], popup='<strong> HUMBERTO DE CAMPOS</strong>',
                                        tooltip=tooltip_COOP_HCAMPOS,
                                        icon=folium.Icon(color='orange', icon='flash')).add_to(m)
# <--- PILHAS E BATERIAS FIM--->

# print
m.save('map-rp-markers-17-nov.html')