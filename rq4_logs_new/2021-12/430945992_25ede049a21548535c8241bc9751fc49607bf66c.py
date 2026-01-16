
repeticion = False
companion = ""
nafta = False
arrancar = False
yerba = False
aceite = False
aire = False
vamos = False
hora = ""
musica = ""
import time
import sys
a = 1
b = 1
c = 0.06
d = 0.3

print()
print("=================================================")
print("=                _.---------._                  =")
print("=              ,'.-----------.',                =")
print("=             /='-------------`=\               =")
print("=           .F_______....._______Y.             =")
print("=           |(_)(_) _______ (_)(_)|             =")
print("=           (....__|RTS2021|__....)             =")
print("=            | |    ~~~~~~~    | |              =")
print("=            `-'               `-'              =")
print("=      Bienvenidos a Road Trip Simulator!       =")
print("=================================================")
print()
time.sleep(a)

while True:

    def path6():
        time.sleep(b)
        print()
        charla = input("De qué quieres hablar? (planes/salud/viaje) ")

        if charla == "planes" or charla == "p":
            print()
            d2 = '"Desde que volví a Buenos Aires me costó adaptarme un poco...'
            for character in d2:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            d3 = 'Encontrar donde vivir, volver a conseguir trabajo, recuperar mis vínculos.'
            for character in d3:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            d4 = 'Una vez que volví a acomodarme todo fue más fácil.'
            for character in d4:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            d5 = 'Mis planes? '
            for character in d5:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            d5a = 'Te diré cuando los sepa!"'
            for character in d5a:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            d6 = '"Pero seguro que tengo algunas ideas :)"'
            for character in d6:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            path6()
        elif charla == "salud" or charla == "s":
            print()
            d7 = '"No ha sido fácil..."'
            for character in d7:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            path6()
        elif charla == "viaje" or charla == "v":
            print()
            d8 = '"Por qué vine? '
            for character in d8:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            d9 = 'Cuando me invitaste estaba en uno de esos momentos... digamos que necesitaba tomar aire..."'
            for character in d9:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            path6()
        else:
            print()
            print("Mejor pregunta algo diferente.")
            path6()

        print()
        print("================================================")
        print()

    def path5():
        time.sleep(b)
        print()
        print("Antes de darnos cuenta ya estamos avanzando por la Ruta 2.")
        print("La charla fluye mientras pasan los temas de " + musica + ", y " + companion + " resultó ser la buena compañía que esperabas.")
        path6()

    def path4():
        radio = input("Quieres cambiar la música? (si/no) ").lower().strip()
        if radio[0] == "s":
            if companion == "juanma" or companion == "Juanma" or companion == "JUANMA":
                time.sleep(b)
                print()
                j4 = companion + ': "Ah... bueno..."'
                for character in j4:
                    sys.stdout.write(character)
                    sys.stdout.flush()
                    time.sleep(c)
                print()
                pathJ()
            else:
                path3()
        elif radio[0] == "n":
            path5()
        else:
            path3()


    def pathJ():
        global musica
        time.sleep(b)
        print()
        musica = input("Qué deseas escuchar? ").lower().strip()
        if musica == "red hot chili peppers" or musica == "rhcp" or musica == "red hot" or musica == "chili peppers" or musica == "red hot chili pepers":
            time.sleep(b)
            musica1 = "♫ Suena 'Scar Tissue', de los Red Hot Chili Peppers. ♫"
            print()
            print(musica1)
            time.sleep(b)
            print()
            j2 = (companion + ': "Altísima banda, no? Uh es un discazo este, un tema mejor que el otro."')
            for character in j2:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            j3 = '        "Escuchá lo que es este solo de Frusciante, una locura."'
            for character in j3:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            musica = "los Red Hot Chili Peppers"
            time.sleep(b)
            print()
            print()
            path4()
        else:
            time.sleep(b)
            musica1 = ': "Ah no querés escuchar los Red Hot...? Otra cosa? Uh perdón no traje nada, pensé que no íbamos a necesitar."'
            print()
            j = (companion + musica1)
            for character in j:
                sys.stdout.write(character)
                sys.stdout.flush()
                time.sleep(c)
            print()
            pathJ()   

    def path3a():
        if companion == "juanma" or companion == "Juanma" or companion == "JUANMA":
            pathJ()
        else:
            path3()



    def path3():
        global musica
        time.sleep(b)
        print()
        musica = input("Qué deseas escuchar? ").lower().strip()

        if musica == "red hot chili peppers" or musica == "rhcp" or musica == "red hot" or musica == "chili peppers" or musica =="red hot chilli peppers" or musica =="red hot chili pepers":
                time.sleep(b)
                musica1 = "♫ Suena 'Scar Tissue'. ♫ "
                print()
                print(musica1)
                print("No hay otro tema mejor para un viaje en la ruta.")
                print()
                musica = "los Red Hot Chili Peppers"
                path4()
        elif musica == "strokes" or musica == "the strokes":
            time.sleep(b)
            musica1 = "♫ Suena 'You Only Live Once.' ♫"
            print()
            print(musica1)
            print("Qué gran banda!")
            print()
            musica = "The Strokes"
            path4()
        elif musica == "bowie" or musica == "david bowie":
            time.sleep(b)
            musica1 = "♫ Suena 'The Man Who Sold The World'. ♫"
            print()
            print(musica1)
            print("Leyenda.")
            print()
            musica = "Bowie"
            path4()
        elif musica == "beatles" or musica == "the beatles" or musica == "los beatles":
            time.sleep(b)
            musica1 = "♫ Suena 'Strawberry Fields Forever'. ♫"
            print()
            print(musica1)
            print("Living is easy with eyes closed.")
            print()
            musica = "los Beatles"
            path4()
        elif musica == "taylor swift" or musica == "taylor" or musica == "swift":
            time.sleep(b)
            musica1 = "♫ Suena 'I Knew You Were Trouble (Taylor's Version)'. ♫"
            print()
            print(musica1)
            print("Me pregunto qué habrá sido de esa bufanda.")
            print()
            musica = "Taylor Swift"
            path4()
        elif musica == "queen" or musica == "freddie" or musica == "freddie mercury" or musica == "freddy":
            time.sleep(b)
            musica1 = "♫ Suena 'Don't Stop Me Now'! ♫"
            print()
            print(musica1)
            print("Y el auto parece un karaoke.")
            print()
            musica = "Queen"
            path4()
        elif musica == "gorillaz" or musica == "gorilaz":
            time.sleep(b)
            musica1 = "♫ Suena 'Feel Good Inc'. ♫"
            print()
            print(musica1)
            print("Feel Good.")
            print()
            musica = "Gorillaz"
            path4()
        elif musica == "coldplay":
            time.sleep(b)
            musica1 = "♫ Suena 'The Scientist'. ♫"
            print()
            print(musica1)
            print("Come up to meet you, tell you I'm sorryyyyy, you don't know how lovely you aaaaaaaaare. Justo en el cora.")
            print()
            musica = "Coldplay"
            path4()
        elif musica == "dua lipa" or musica == "dualipa" or musica == "dua":
            time.sleep(b)
            musica1 = "♫ Suena 'Love Again'. ♫"
            print()
            print(musica1)
            print("Nada mal.")
            print()
            musica = "Dua Lipa"
            path4()
        elif musica == "bersuit" or musica == "bersuit vergarabat":
            time.sleep(b)
            musica1 = "♫ Suena 'Toco y Me Voy'. ♫"
            print()
            print(musica1)
            print("Escuchaba mucho esta banda en sus buenos días.")
            print()
            musica = "Bersuit"
            path4()
        elif musica == "redondos" or musica == "patricio rey" or musica == "los redondos" or musica == "indio" or musica == "indio solari":
            time.sleep(b)
            musica1 = "♫ Suena 'Ella Debe Estar Tan Linda'. ♫"
            print()
            print(musica1)
            print("Conduje toda la noche! Reventando los cambios! Con mis ojos de durax las ti ma dos! Qué banda.")
            print()
            musica = "los Redondos"
            path4()
        elif musica == "wos" or musica == "wosito":
            time.sleep(b)
            musica1 = "♫ Suena 'Melón Vino.' ♫"
            print()
            print(musica1)
            print("Tengo estudio y un colchón, tengo amigos un montón.")
            print()
            musica = "Wos"
            path4()
        elif musica == "rolling stones" or musica == "stones" or musica == "rolling" or musica == "los stones":
            time.sleep(b)
            musica1 = "♫ Suena 'Start Me Up.' ♫"
            print()
            print(musica1)
            print()
            musica = "los Rolling Stones"
            path4()
        elif musica == "billie" or musica == "billie eilish" or musica == "bilie eilish" or musica == "bilie":
            time.sleep(b)
            musica1 = "♫ Suena 'Whish You Were Gay.' ♫"
            print()
            print(musica1)
            print()
            path4()
        elif musica == "cowboy bebop" or musica == "yoko kanno" or musica == "seatbelts" or musica == "the seatbelts":
            time.sleep(b)
            musica1 = "♫ Suena 'TANK.' ♫"
            print()
            print(musica1)
            print("OK 3, 2, 1 LETS JAM")
            print()
            musica = "Cowboy Bebop"
            path4()
        elif musica == "evangelion" or musica == "opening evangelion" or musica == "neon genesis evangelion" or musica == "zankoku na tenshi" or musica == "zankoku":
            time.sleep(b)
            musica1 = "♫ Suena el opening de Evangelion, 'Cruel Angel's Thesis' ♫"
            print()
            print(musica1)
            print("Congratulations!")
            print()
            musica = "Evangelion"
            path4()

        elif musica == "CAMBIAR" or musica == "CAMBIARR" or musica == "CAMBIARRR":
            musica1 = "♫ Suena 'CAMBIAR.' ♫"
            print()
            print(musica1)
            print("Placeholder")
            print()
            path4()
            
        elif musica == "rock" or musica == "pop" or musica == "cumbia" or musica == "punk" or musica == "musica clasica" or musica == "clasica" or musica == "metal" or musica == "cuarteto":
            time.sleep(b)
            musica1 = "Qué banda?"
            print(musica1)
            path3()
                
        else:
            time.sleep(b)
            musica1 = "No hay señal y no traje conmigo nada de esa banda."
            print()
            print(musica1)
            path3()      




    def path2():
        global hora
        print()
        hora = input("El viaje comenzó a la (mañana/tarde/noche) ").lower().strip()
        time.sleep(b)
        print()
        if hora != "mañana" and hora != "tarde" and hora != "noche" and hora != "m" and hora != "t" and hora != "n":
            path2()
        else:
            if hora == "mañana" or hora == "m":
                hora = "mañana"
                print("Fue la decisión correcta. Hay poco tráfico y estoy despierto para manejar.")
                print("Tal vez podríamos desayunar algo en ese conocido parador cuando lleguemos ahí.")
                print("La " + hora + " es perfecta para poner un poco de música")
                path3a()
            elif hora == "tarde" or hora == "t":
                hora = "tarde"
                print("Podríamos haber salido antes, pero los preparativos llevaron más tiempo del esperado.")
                print("Hay tráfico. La fila de autos llega hasta donde la vista puede alcanzar.")
                print("Tal vez podríamos detenernos en el conocido parador de la ruta y esperar que cambie un poco.")
                print("La " + hora + " es perfecta para poner un poco de música")
                path3a()
            else:
                hora = "noche"
                print("Creo que fue la mejor decisión. Casi no hay otros autos, la mayoría seguro saldrá mañana.")
                print("Nos deslizamos por la autopista rápidamente, y ya pronto estaremos en la ruta.")
                print("Tal vez luego podemos tomar un café en ese conocido parador.")
                print("La " + hora + " es perfecta para poner un poco de música")
                path3a()


    def path1():
        global nafta
        global arrancar
        global yerba
        global vamos
        global aire
        global aceite
        secondPath = input("Qué deseas hacer? No olvides nada! (nafta/cubiertas/aceite/vamos!/.../ayuda) ").lower().strip()


        if secondPath == "arrancar" or secondPath == "encender" or secondPath == "poner en marcha" or secondPath == "encender auto" or secondPath == "contacto" or secondPath == "marcha" or secondPath == "llave" or secondPath == "prender" or secondPath == "x":
            if arrancar == True:
                print()
                print("El motor ya está en marcha. Listos para salir.")
                path1()
            else:
                print()
                print("Enciendes el auto y el motor vibra suavemente. Estamos listos para partir!")
                arrancar = True
                path1()

        elif secondPath == "nafta" or secondPath == "n":
            if nafta == True:
                print()
                print("Tanque lleno.")
                path1()
            else:
                print()
                print("Llenas el tanque, siempre es una buena idea antes de viajar.")
                nafta = True
                path1()

        elif secondPath == "cubiertas" or secondPath == "c" or secondPath == "aire":
            if aire == True:
                print()
                print("Aire listo.")
                path1()
            else:
                print()
                print("Chequeas las cubiertas en la estación de servicio, les agregas un poco de aire.")
                aire = True
                path1()

        elif secondPath == "aceite" or secondPath == "a":
            if aceite == True:
                print()
                print("Agua y aceite llenos.")
                path1()
            else:
                print()
                print("Levantas el capot y agregas un poco de agua y aceite.")
                aceite = True
                path1()

        elif secondPath == "yerba" or secondPath == "mate" or secondPath == "bombilla":
            if yerba == True:
                print()
                print("Todo listo para el mate.")
                path1()
            else:
                print()
                print("Mejor no olvidar la yerba! Un viaje no es un viaje sin unos buenos mates, eso dicen.")
                yerba = True
                path1()

        elif secondPath == "...":
                print()
                print("► Prueba diferentes cosas que te sirvan en tu viaje, o puedes escribir 'ayuda'. ◄")
                path1()

        elif secondPath == "ayuda" or secondPath == "help":
                time.sleep(b)
                print()
                print("===================================================================================================")
                print("Road Trip Simulator - 2021")
                print("Autor: Guido Cano")
                print("===================================================================================================")
                print("► Bienvenidos a RTS! ◄")
                print("Falta poco para salir! Prueba escribir diferentes cosas que te ayudarán en tu viaje")
                print("Intenta con términos simples, de una o dos palabras.")
                print("Si no sabes cómo seguir, tal vez sea una buena idea subirse al auto y ponerlo en marcha.")
                print("Presiona 'ayuda' cada vez que lo necesites en el juego.")
                print("Espero que lo disfrutes!")
                print("===================================================================================================")
                time.sleep(b)
                print()
                path1()                   


        elif secondPath == "vamos" or secondPath == "salir" or secondPath == "vamos!" or secondPath == "salir" or secondPath == "partir" or secondPath == "v":
            if vamos == True and arrancar == False:
                print()
                print("Ya están sentados y listos. El auto no está en marcha.")
                path1()
            elif arrancar == True:
                time.sleep(b)
                print()
                print("Todo listo!!")
                time.sleep(b)
                print()
                print("El auto acelera por Avenida San Juan y se abre paso por las calles de Buenos Aires.")
                print("Pronto están en la 9 de Julio subiendo a la autopista.")
                print("Aún con las ventanillas altas se escucha el tranquilizador ruido del asfalto.")
                print(companion + " bromea sobre una imágen que ve en su celular.")
                print("La imágen compara un perro grande y un perro chiquito con diferentes tipos de plantas.")
                print("Hace una voz graciosa:")
                print('"Me regaste y el agua estaba muy fría, ahora debo morir." Ambos ríen.')
                print("Memes, el nuevo lenguaje universal.")
                time.sleep(b)
                path2()
            else:
                print()
                print("Se suben al auto. Por dentro también reluce, y huele a lavanda. No está en marcha.")
                vamos = True
                path1()
                   
        else:
            path1()



    def choose1():
        global repeticion
        path1 = ""
        while path1 != "si" and path1 != "no" and path1 != "n" and path1 != "s":
            if repeticion == True:
                path1 = input ("Quieres volver a viajar? (si/no) ").lower().strip()
            else:
                path1 = input ("Quieres comenzar un viaje? (si/no) ").lower().strip()
        return path1[0]

    if choose1() == "s":
        repeticion = True
        print()
        time.sleep(b)
        print("Te decides y comienzas con los preparativos!")
        print("Realizas los controles necesarios al auto y le haces una visita a tu mecánico de confianza.")
        print("Pasas por el lavadero y te tomas un cafe mientras terminan el trabajo, mirando a la gente pasar por Avenida Rivadavia.")
        print("Pronto tendrás unos días libres, y has decidido pasarlos en la costa.")
        print()
        time.sleep(b)
        companion = input("Desde el comienzo sabías a quién le dirías que te acompañe. (Nombre) ")
        print()
        time.sleep(b)
        print("Se lo preguntaste una tarde entre mate y mate, y " + companion + " aceptó de inmediato.")
        print()
        time.sleep(b)
        d1 = '"Un viaje a la costa, y manejás vos?'
        for character in d1:
            sys.stdout.write(character)
            sys.stdout.flush()
            time.sleep(c)
        time.sleep(d)
        d2 = ' Dónde firmo?"'
        for character in d2:
            sys.stdout.write(character)
            sys.stdout.flush()
            time.sleep(c)
        print()
        print()
        print("---")
        print()
        time.sleep(b)
        print("Llegó el día, y ya está casi todo listo.")
        print("Pasas por la casa de " + companion + " y cargan su bolso en el baúl.")
        print()
        path1()
    else:
        print("Será en otro momento!")
        print()