import pyxel
WIDTH = 128
HEIGHT = 128
X = 0
Y = 1
PLAYER_POS = (16, 0)
MUR = (4, 2)
MUR2 = (5, 2)
SPAWN = (0, 1)
SOL = (0, 0)
PLAT = (6, 2)
CHEST = (6, 0)
MUR_EAU = (4, 3)
EAU = (1, 0)
MOB_POS = (0, 3)
MINE = (2, 2)
MUNITION = (5,3)
COEUR = (4, 4)
ALGUE = (2, 4)
BAS = 0
HAUT = 1
DROITE = 2
GAUCHE = 3
MUN_U = 50
MUN_V = 25
MUR_CASSABLE_T = (4,6)
MUR_CASSABLE_A = (4,5)



class Map:
    def __init__(self):
        self.niv = 0
        self.u = 0 + 128 * self.niv
        self.v = 0
        self.tilemap = pyxel.tilemap(0)
        self.cache = pyxel.tilemap(1)
        self.spawn = None
        self.chest = None
        self.fin = False
        self.max_niv = 15
        self.pos_les_mobs_terrestre = []
        self.pos_les_mobs_aquatique = []
        self.pos_les_coeur_terrestre = []
        self.pos_les_coeur_aqua = []
        self.pos_les_algues = []



        self.pos_les_mines = []
        self.pos_les_mun = []

        self.parcour()

    def draw(self):
        pyxel.bltm(0, 0, 0, self.u, self.v, WIDTH, HEIGHT)
        pyxel.bltm(0, 0, 1, self.u, self.v, WIDTH, HEIGHT, colkey=0)

    def in_bloc(self, x, y, bloc):
        x += self.niv * 128
        if self.tilemap.pget(x // 8, y // 8) == bloc:
            return True
        return False

    def in_bloc_calque_2(self,x,y,bloc):
        x += self.niv * 128
        if self.cache.pget(x // 8, y // 8) == bloc:
            return True
        return False


    def in_bloc_sous_l_eau(self, x, y, bloc):
        x += self.niv * 128
        if self.tilemap.pget(x // 8, (y + 128) // 8) == bloc:
            return True
        return False

    def in_bloc_sous_l_eau_calque2(self, x, y, bloc):
        x += self.niv * 128
        if self.cache.pget(x // 8, (y + 128) // 8) == bloc:
            return True
        return False


    def afficher_l_eau(self, vrai):
        if vrai:
            self.v = 128
        else:
            self.v = 0

    def get_bloc(self, x, y, bloc):
        if self.tilemap.pget(x, y) == bloc:
            return True
        return False

    def parcour(self):
        self.pos_les_mobs_terrestre.clear()
        self.pos_les_mobs_aquatique.clear()
        self.pos_les_mines.clear()
        self.pos_les_mun.clear()
        self.pos_les_coeur_terrestre = []
        self.pos_les_coeur_aqua = []
        self.pos_les_algues = []

        for dx in range(WIDTH // 8):
            for dy in range(HEIGHT // 8):
                ddx = dx + self.niv * 16
                if self.get_bloc(ddx, dy, SPAWN):
                    self.spawn = (dx * 8, dy * 8)
                    self.cache.pset(ddx, dy, SOL)
                if self.get_bloc(ddx, dy, CHEST):
                    self.chest = (dx * 8, dy * 8)
                    self.cache.pset(ddx, dy, SOL)
                if self.get_bloc(ddx, dy, MOB_POS):
                    self.pos_les_mobs_terrestre.append((dx * 8, dy * 8))
                    self.cache.pset(ddx, dy, SOL)
                if self.get_bloc(ddx, dy + 16, MOB_POS):
                    self.pos_les_mobs_aquatique.append((dx * 8, dy * 8))
                    self.cache.pset(ddx, dy + 16, EAU)
                if self.get_bloc(ddx, dy, MINE):
                    self.pos_les_mines.append((dx * 8, dy * 8))
                    self.cache.pset(ddx, dy, SOL)

                if self.get_bloc(ddx, dy + 16, MUR_CASSABLE_A):
                    self.cache.pset(ddx, dy + 16, MUR_CASSABLE_A)
                if self.get_bloc(ddx, dy, MUR_CASSABLE_T):
                    self.cache.pset(ddx, dy, MUR_CASSABLE_T)

                if self.get_bloc(ddx, dy, COEUR):
                    self.pos_les_coeur_terrestre.append((dx * 8, dy * 8))
                    self.cache.pset(ddx, dy, SOL)

                if self.get_bloc(ddx, dy + 16, COEUR):
                    self.pos_les_coeur_aqua.append((dx * 8, dy * 8))
                    self.cache.pset(ddx, dy + 16, EAU)

                if self.get_bloc(ddx, dy + 16, ALGUE):
                    self.pos_les_algues.append((dx * 8, dy * 8))
                    self.cache.pset(ddx, dy + 16, EAU)

                if self.get_bloc(ddx, dy, MUNITION):
                    self.pos_les_mun.append((dx * 8, dy * 8))
                    self.cache.pset(ddx, dy, SOL)



    def next_level(self):
        self.niv += 1
        self.u = 128 *self.niv
        if self.niv == self.max_niv+1:
            self.fin = True
        else:
            self.parcour()

class Player:
    def __init__(self, spawn):
        self.x = 0
        self.y = 0
        self.spawn = spawn
        self.retourne_spawn()
        self.u = PLAYER_POS[X]
        self.v = PLAYER_POS[Y]
        self.W = 8
        self.H = 8
        self.speed = 8
        self.oxygene_max = 60 * 5
        self.oxygene = self.oxygene_max
        self.est_sous_l_eau = False
        self.G = 0.5
        self.f = 0
        self.dead = False
        self.pvmax = 60 * 3
        self.pv = self.pvmax
        self.orientation = HAUT
        self.mun = 3
        self.mun_max = 3
        self.message = None

    def proj_count(self):
        munx = 4
        muny = HEIGHT - 11
        for mun in range(self.mun_max):
            pyxel.blt(munx, muny, 0, MUN_U, MUN_V + 8, 4, 7,0 )
            munx += 5
        munx = 4
        for mun in range(self.mun):
            pyxel.blt(munx, muny, 0, MUN_U, MUN_V, 4, 7, 0 )
            munx += 5



    def retourne_a_la_surface(self, map):
        if self.collide(map):
            self.message = ["Mur au dessu", 0]
        else:
            self.est_sous_l_eau = False
            pyxel.stop()
            pyxel.playm(1, loop=True)
            self.u, self.v = 16, 0
            self.x = self.x // 8*8
            self.y = self.y // 8*8

    def take_dammage(self, cmb):
        self.pv -= int(cmb)
        if self.pv < 0:
            self.dead = True
            self.pv = 0

    def affiche_pv(self):
        pour = int(self.pv / self.pvmax * 100)
        pyxel.text(0, 0, "pv: " + str(pour) + "%", 0)

    def revie(self):
        self.pv = self.pvmax
        self.retourne_spawn()
        self.oxygene = self.oxygene_max
        self.est_sous_l_eau = False
        self.u, self.v = 16, 0
        self.dead = False
        pyxel.stop()
        pyxel.playm(1, loop=True)

    def retourne_spawn(self):
        self.x = self.spawn[X]
        self.y = self.spawn[Y]

    def draw(self):
        pyxel.blt(self.x, self.y, 0, self.u, self.v, self.W, self.H, colkey=0)

    def collide(self, map):
        # pos = [(self.x,self.y),(self.x + self.W-1,self.y),(self.x,self.y+self.H),(self.x+self.W-1,self.y+self.H-1)]
        if map.in_bloc(self.x, self.y, MUR):
            return True
        if map.in_bloc(self.x, self.y, MUR2):
            return True
        if map.in_bloc_calque_2(self.x,self.y,MUR_CASSABLE_T):
            return True
        return False

    def collide_eau(self, map):
        pos = [(self.x, self.y), (self.x + self.W - 1, self.y), (self.x, self.y + self.H - 1),
               (self.x + self.W - 1, self.y + self.H - 1)]
        for i in pos:
            if map.in_bloc_sous_l_eau(i[0], i[1], PLAT):
                return True
            if map.in_bloc_sous_l_eau(i[0], i[1], MUR_EAU):
                return True
            if map.in_bloc_sous_l_eau_calque2(i[0], i[1], MUR_CASSABLE_A):
                return True
        return False

    def update(self, map):
        self.move(map)
        self.vas_sous_l_eau(map)
        self.respire()
        if self.oxygene<=0:
            self.take_dammage(5)
        if self.oxygene <= 0:
            self.retourne_a_la_surface(map)

        if self.y > 128:
            self.dead = True

    def draw_barre_oxygene(self):
        if self.est_sous_l_eau or self.oxygene < self.oxygene_max:
            pourcentage = self.oxygene / self.oxygene_max
            largeur_barre = 4
            position_barre = ((WIDTH - largeur_barre),4)
            HAUTEUR_BARRE = HEIGHT - largeur_barre*2
            size = int((HAUTEUR_BARRE-2) * pourcentage)
            pyxel.rect(position_barre[X],position_barre[Y], largeur_barre, HAUTEUR_BARRE, 13)
            y = (largeur_barre+1+HAUTEUR_BARRE-2) - size
            pyxel.rect(position_barre[X]+1, y, largeur_barre-2, size, 5)

    def respire(self):
        if self.est_sous_l_eau:
            self.oxygene -= 1
            if self.oxygene < 0: self.oxygene = 0
        else:
            if self.oxygene < self.oxygene_max:
                self.oxygene += 2
                if self.oxygene > self.oxygene_max:
                    self.oxygene = self.oxygene_max

    def move(self, map):
        if (pyxel.btnp(pyxel.KEY_SPACE) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_B)) and self.est_sous_l_eau:
            self.f = -4
        if not self.est_sous_l_eau:

            dx = 0
            dy = 0
            if pyxel.btnp(pyxel.KEY_LEFT) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_LEFT):
                self.u, self.v = 32, 8
                self.orientation = GAUCHE
                dx = - self.speed
                dy = 0
            if pyxel.btnp(pyxel.KEY_RIGHT) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_RIGHT):
                self.u, self.v = 32, 0
                self.orientation = DROITE
                dx = self.speed
                dy = 0
            if pyxel.btnp(pyxel.KEY_UP) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_UP):
                self.orientation = HAUT
                self.u, self.v = 16, 0
                dx = 0
                dy = - self.speed
            if pyxel.btnp(pyxel.KEY_DOWN) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_DOWN):
                self.orientation = BAS
                self.u, self.v = 16, 8
                dx = 0
                dy = self.speed

            self.x += dx
            self.y += dy

            if self.collide(map):
                self.x -= dx
                self.y -= dy

            if self.x >= WIDTH: self.x = self.x - self.W

        else:
            if pyxel.frame_count % 2 == 0:
                self.f += self.G
                for dy in range(abs(int(self.f))):
                    self.y += (self.f / abs(self.f))
                    if self.collide_eau(map):
                        self.y -= (self.f / abs(self.f))
                        self.f = 0
                        break
                self.y = int(self.y)

            dx = 0
            if pyxel.btn(pyxel.KEY_RIGHT) or pyxel.btn(pyxel.GAMEPAD1_BUTTON_DPAD_RIGHT):
                dx = 1
                self.u, self.v = 0, 32
                self.orientation = DROITE

            if pyxel.btn(pyxel.KEY_LEFT) or pyxel.btn(pyxel.GAMEPAD1_BUTTON_DPAD_LEFT):
                self.u, self.v = 0, 40
                dx = -1
                self.orientation = GAUCHE

            self.x += dx
            if self.collide_eau(map):
                self.x -= dx

        if self.x <= 0: self.x = 0
        if self.y - self.H >= HEIGHT: self.y = self.y - self.H
        if self.y <= 0: self.y = 0

    def vas_sous_l_eau(self, map):
        if (pyxel.btnp(pyxel.KEY_C) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_A)):
            if self.est_sous_l_eau:
                self.retourne_a_la_surface(map)
            else:
                if self.collide_eau(map):
                    self.message = ["Sol trop dur", 0]
                else:
                    self.est_sous_l_eau = True
                    pyxel.stop()
                    pyxel.playm(0, loop=True)
                    self.u, self.v = 0, 32
                    self.orientation = DROITE

class Cheste:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def draw(self,etape,orientation):
        co = {
            BAS:(0,8),
            HAUT: (0, -8),
            GAUCHE: (-8, 0),
            DROITE: (8, 0)
        }
        x = self.x + co[orientation][X]
        y = self.y + co[orientation][Y]
        if etape == 0:
            pyxel.blt(x,y,0,56,0,8,8,colkey=0)
        if etape == 1:
            pyxel.blt(x,y,0,48,8,8,8,colkey=0)
        if etape == 2:
            pyxel.blt(x,y,0,48,0,8,8,colkey=0)
class Explo:
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.delay_anim = 30
        self.timer = 0

    def est_fini(self):
        if self.timer >= self.delay_anim:
            return True
        return False

    def draw(self):
        self.timer += 1
        x = self.x
        y = self.y
        if self.timer <= self.delay_anim // 2:
            pyxel.blt(x, y, 0, 56, 16, 8, 8, colkey=0)
        if self.timer >= self.delay_anim // 2:
            pyxel.blt(x, y, 0, 56, 8, 8, 8, colkey=0)
class Mob:
    def __init__(self, x, y, version):
        self.x = x
        self.y = y
        self.W = 8
        self.H = 8
        self.version = version
        self.u = 0 if version == 0 else 16
        self.v = 24 if version == 0 else 40
        self.spawn = (x, y)

    def draw(self, dans_leau):
        if self.version == dans_leau:
            pyxel.blt(self.x, self.y, 0, self.u, self.v, self.W, self.H, colkey=0)

    def update(self):
        if pyxel.frame_count % 5 == 0:
            self.x += pyxel.rndi(-1, 1)
            self.y += pyxel.rndi(-1, 1)

            if self.x > self.spawn[X] + 3 * 8:
                self.x = self.spawn[X] + 3 * 8

            if self.x < self.spawn[X] - 3 * 8:
                self.x = self.spawn[X] - 3 * 8

            if self.y > self.spawn[Y] + 3 * 8:
                self.y = self.spawn[Y] + 3 * 8

            if self.y < self.spawn[Y] - 3 * 8:
                self.y = self.spawn[Y] - 3 * 8

class Mines:
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.W = 8
        self.H = 8
        self.u = 16
        self.v = 16

    def draw(self, vrai):
        if vrai:
            pyxel.blt(self.x, self.y, 0, self.u, self.v, self.W, self.H, colkey=0)

class Algue:
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.W = 8
        self.H = 8
        self.u = [16, 24]
        self.v = 32
        self.index = 0
        self.delay = 5

    def draw(self, est_sou_l_eau: bool) -> None:
        if est_sou_l_eau:
            if pyxel.frame_count % self.delay == 0:
                self.index = (self.index + 1) % len(self.u)
            pyxel.blt(self.x, self.y, 0, self.u[self.index], self.v, self.W, self.H, colkey=0)

class Coeur:
    def __init__(self, pos):
        self.x = pos[0]
        self.y = pos[1]
        self.W = 8
        self.H = 8
        self.U = 32
        self.V = 32

    def draw(self):
        pyxel.blt(self.x, self.y, 0, self.U, self.V, self.W, self.H, colkey=0)
class Pop_mun:
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.W = 8
        self.H = 8
        self.u = 40
        self.v = 24

    def draw(self):
        pyxel.blt(self.x, self.y, 0, self.u, self.v, self.W, self.H, colkey=0)


class Les_entite:
    def __init__(self):
        self.les_mobs_terre = []
        self.les_mobs_aqua = []
        self.les_mines = []
        self.les_coeur_terrestre = []
        self.les_coeur_aqua = []
        self.les_algues = []
        self.les_mun = []
        self.mon_explo = None

    def kill_mob(self, projectile, collide):
        index_a_sup = []
        index = 0
        if not projectile.est_sous_l_eau:
            for mob in self.les_mobs_terre:
                if collide(projectile, mob):
                    index_a_sup.append(index)
                index += 1
            for mob in index_a_sup[0::-1]:
                self.les_mobs_terre.pop(mob)
        else:
            for mob in self.les_mobs_aqua:
                if collide(projectile, mob):
                    index_a_sup.append(index)
                index += 1
            for mob in index_a_sup[0::-1]:
                self.les_mobs_aqua.pop(mob)
        return False if len(index_a_sup)<=0 else True
    def spawn_mob(self, tilemap) -> None:
        self.les_mobs_terre.clear()
        self.les_mobs_aqua.clear()
        self.les_coeur_aqua.clear()
        self.les_coeur_terrestre.clear()
        self.les_mines.clear()
        self.les_algues.clear()
        self.les_mun.clear()
        for i in tilemap.pos_les_mobs_terrestre:
            self.les_mobs_terre.append(Mob(i[X], i[Y], 0))
        for i in tilemap.pos_les_mobs_aquatique:
            self.les_mobs_aqua.append(Mob(i[X], i[Y], 1))
        for i in tilemap.pos_les_mines:
            self.les_mines.append(Mines(i[X], i[Y]))
        for i in tilemap.pos_les_coeur_terrestre:
            self.les_coeur_terrestre.append(Coeur((i[X], i[Y])))
        for i in tilemap.pos_les_coeur_aqua:
            self.les_coeur_aqua.append(Coeur((i[X], i[Y])))
        for i in tilemap.pos_les_algues:
            self.les_algues.append(Algue(i[X], i[Y]))
        for i in tilemap.pos_les_mun:
            self.les_mun.append(Pop_mun(i[X], i[Y]))

    def update(self, player, collision):
        for mo in self.les_mobs_terre:
            mo.update()
            if collision(mo, player) and not player.est_sous_l_eau:
                player.take_dammage(2)

        for mo in self.les_mobs_aqua:
            mo.update()

            if collision(mo, player) and player.est_sous_l_eau:
                player.take_dammage(2)

        m = 0
        for mun in self.les_mun:
            if not player.est_sous_l_eau:
                if collision(mun, player):
                    player.mun = 3
                    self.les_mun.pop(m)
                    break
            m  +=1
        for mi in self.les_mines:
            if collision(mi, player):
                self.mon_explo = Explo(mi.x, mi.y)
                pyxel.stop()
                pyxel.play(0,2)

        i = 0
        for co in self.les_coeur_terrestre:
            if collision(player, co):
                player.pv = player.pvmax
                self.les_coeur_terrestre.pop(i)
        i += 1
        i = 0
        for co in self.les_coeur_aqua:
            if collision(player, co):
                player.pv = player.pvmax
        i += 1

    def draw(self, player):
        for mo in self.les_mobs_terre:
            mo.draw(0 if not player.est_sous_l_eau else 1)
        for mo in self.les_mobs_aqua:
            mo.draw(0 if not player.est_sous_l_eau else 1)
        for mi in self.les_mines:
            mi.draw(player.est_sous_l_eau)
        for co in self.les_coeur_terrestre:
            if not player.est_sous_l_eau:
                co.draw()
        for co in self.les_coeur_aqua:
            if player.est_sous_l_eau:
                co.draw()
        for al in self.les_algues:
            al.draw(player.est_sous_l_eau)
        for mun in self.les_mun:
            if not player.est_sous_l_eau:
                mun.draw()

class Proj:
    def __init__(self,player):
        self.orientation = player.orientation
        self.W,self.H = (1,2) if self.orientation in (BAS,HAUT) else (2,1)
        self.x = player.x + player.W // 2
        self.y = player.y + player.H //2
        self.u, self.v = (3,22) if self.orientation in (BAS,HAUT) else (22,28)
        self.est_sous_l_eau = player.est_sous_l_eau

    def move(self):
        if self.orientation == GAUCHE:
            self.x -= 1
        if self.orientation == DROITE:
            self.x += 1
        if self.orientation == BAS:
            self.y += 1
        if self.orientation == HAUT:
            self.y -= 1

    def draw(self,est_sous_l_eau):
        if self.est_sous_l_eau and est_sous_l_eau:
            pyxel.blt(self.x,self.y,0,self.u,self.v,self.W,self.H,0)
        if not self.est_sous_l_eau and not est_sous_l_eau:
            pyxel.blt(self.x,self.y,0,self.u,self.v,self.W,self.H,0)

class Les_projectile:
    def __init__(self):
        self.les_proj = []

    def update(self,les_entite,collide,est_dans_l_eau,map):
        index = 0
        a_sup = []
        for i in self.les_proj:
            i.move()
            if les_entite.kill_mob(i,collide):
                a_sup.append(index)
            if i.x < 0 or i.x > WIDTH or i.y <0 or i.y >HEIGHT:
                a_sup.append(index)

            if not i.est_sous_l_eau:
                if map.in_bloc_calque_2(i.x,i.y,MUR_CASSABLE_T):
                    map.cache.pset((i.x+map.niv*128)//8,i.y//8,SOL)
                    a_sup.append(index)

                if map.in_bloc(i.x, i.y, MUR) or map.in_bloc(i.x, i.y, MUR2):
                    a_sup.append(index)
            else:
                if map.in_bloc_sous_l_eau_calque2(i.x,i.y,MUR_CASSABLE_A):
                    map.cache.pset((i.x+map.niv*128)//8,(i.y+128)//8,EAU)
                    a_sup.append(index)

                if map.in_bloc_sous_l_eau(i.x, i.y, MUR_EAU):
                    a_sup.append(index)


            index+=1

        a_sup = list(set(a_sup))
        for pro in a_sup[::-1]:
            self.les_proj.pop(pro)
    def draw(self,est_sous_l_eau):
        for i in self.les_proj:
            i.draw(est_sous_l_eau)
    def add(self,player):
        self.les_proj.append(Proj(player))

class Selection:
    def __init__(self, niv):
        self.n_niv = niv
        self.curseur = 0
        self.in_menu = True
        self.mode = None

    def update(self):
        if self.mode == "level":
            if pyxel.btnp(pyxel.KEY_UP) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_UP):
                pyxel.play(3,4)
                self.curseur -= 4

            if pyxel.btnp(pyxel.KEY_DOWN)or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_DOWN):
                pyxel.play(3,4)
                self.curseur += 4

            if pyxel.btnp(pyxel.KEY_RIGHT)or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_RIGHT):
                pyxel.play(3,4)
                self.curseur += 1

            if pyxel.btnp(pyxel.KEY_LEFT)or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_LEFT):
                pyxel.play(3,4)
                self.curseur -= 1

            if self.curseur < 0: self.curseur = 0
            if self.curseur >= self.n_niv: self.curseur = self.n_niv

            if pyxel.btnp(pyxel.KEY_B) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_B):
                self.mode = None
                self.curseur = 0
                pass

            if pyxel.btnp(pyxel.KEY_SPACE) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_A):
                pyxel.stop()
                pyxel.play(3, 5)

                self.in_menu = False
                return self.curseur

            return None
        else:
            if pyxel.btnp(pyxel.KEY_UP) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_UP) or pyxel.btnp(pyxel.KEY_DOWN) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_DPAD_DOWN):
                pyxel.play(3, 4)
                self.curseur = (self.curseur + 1)%2

            if pyxel.btnp(pyxel.KEY_SPACE) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_A):
                if self.curseur == 0:
                    pyxel.stop()
                    pyxel.play(3, 5)
                    self.in_menu = False
                    self.mode = "normal"
                    self.in_menu = False
                    return 0
                else:
                    self.mode = "level"
                    self.curseur = 0

    def draw(self):
        pyxel.cls(5)

        if self.mode == "level":
            x = 16
            y = 16
            ecartement =  16
            taille = 12
            for i in range(self.n_niv+1):
                if self.curseur == i:
                    pyxel.rect(x-1,y-1,taille+2,taille+2,2)

                pyxel.rect(x,y,taille,taille,1)
                pyxel.text(3+x,2+y,str(i),0)

                x += ecartement + taille
                if x > 128-ecartement:
                    x = 16
                    y += ecartement + taille

        else:
            pyxel.rect(32,21,64,32,3 if self.curseur == 0 else 1)
            pyxel.text(52,35,'Normal',0)
            pyxel.rect(32, 74, 64, 32, 3 if self.curseur == 1 else 1)
            pyxel.text(52,88,'Choisir',0)


    def retour_au_menu(self):
        self.in_menu = True
        self.curseur = 0
        pyxel.stop()
        pyxel.playm(1,loop=True)

class Game:
    def __init__(self):
        pyxel.init(WIDTH, HEIGHT)
        pyxel.load("theme2.pyxres")
        self.map = Map()
        self.les_entite = Les_entite()
        self.les_entite.spawn_mob(self.map)
        self.player = Player(self.map.spawn)
        self.chest = Cheste(self.map.chest[0], self.map.chest[1])
        self.les_proj = Les_projectile()
        self.debut = True
        self.dial_niv_5 = False
        self.animation_coffre = False
        self.etape_aniamtion_tresort = 0
        self.timer = 0
        self.select = Selection(self.map.max_niv)
        self.timer_jeu = None
        pyxel.playm(1, loop=True)
        pyxel.run(self.update, self.draw)

    def reset(self, tmp):
        if self.select.mode == "level":
            self.timer_jeu = pyxel.frame_count
            self.map.niv = tmp - 1
        else:
            self.map.niv = -1
            self.timer_jeu = None

        self.player.mun = self.player.mun_max
        self.map.next_level()
        self.player.message = None
        self.player.spawn = self.map.spawn
        self.player.revie()
        self.player.retourne_spawn()
        self.les_entite.spawn_mob(self.map)
        self.chest = Cheste(self.map.chest[0], self.map.chest[1])
        self.les_entite.mon_explo = None

    def update(self):
        if self.select.in_menu:
            tmp = self.select.update()
            if tmp is not None:
                self.reset(tmp)

        else:
            if not self.animation_coffre and self.les_entite.mon_explo is None:
                if self.map.fin:
                    pass
                elif self.map.niv == 4 and not self.dial_niv_5:
                    if pyxel.btnp(pyxel.KEY_SPACE) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_A):
                        self.dial_niv_5 = True
                elif not self.debut:
                    self.player.update(self.map)
                    if self.player.mun >= 1:
                        if pyxel.btnp(pyxel.KEY_S) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_Y):
                            pyxel.play(3, 3)
                            self.les_proj.add(self.player)
                            self.player.mun -= 1
                    self.map.afficher_l_eau(self.player.est_sous_l_eau)
                    if self.player.dead:
                        self.select.mode = None
                        self.select.curseur = 0
                        self.select.in_menu = True
                        self.timer_jeu = None

                    self.les_proj.update(self.les_entite, self.collision, self.player.est_sous_l_eau, self.map)
                    if (self.player.x, self.player.y) == (self.chest.x, self.chest.y) and (
                            pyxel.btnp(pyxel.KEY_D) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_X)):
                        self.animation_coffre = True

                    self.les_entite.update(self.player, self.collision)

                else:
                    if pyxel.btnp(pyxel.KEY_SPACE) or pyxel.btnp(pyxel.GAMEPAD1_BUTTON_A):
                        pyxel.playm(1, loop=True)
                        self.debut = False
            elif self.animation_coffre:
                self.timer += 1
                if self.timer % 15 == 0:
                    self.etape_aniamtion_tresort += 1
                if self.etape_aniamtion_tresort == 4:
                    self.etape_aniamtion_tresort = 0
                    self.timer = 0
                    self.animation_coffre = False
                    if self.select.mode != "level":
                        self.map.next_level()
                        self.player.spawn = self.map.spawn
                        self.player.retourne_spawn()
                        self.les_entite.spawn_mob(self.map)
                        self.chest = Cheste(self.map.chest[0], self.map.chest[1])
                    else:
                        self.select.retour_au_menu()
                        self.timer_jeu = None

            elif self.les_entite.mon_explo is not None:
                if self.les_entite.mon_explo.est_fini():
                    self.les_entite.mon_explo = None
                    self.player.dead = True

    def collision(self, obj1, obj2) -> bool:
        """
        Detect la collision entre l'objet1 et l'objet2
        """
        if obj2.x <= obj1.x <= obj2.x + obj2.W - 1 or obj2.x <= obj1.x + obj1.W - 1 <= obj2.x + obj2.W - 1:
            if obj2.y <= obj1.y <= obj2.y + obj2.H - 1 or obj2.y <= obj1.y + obj1.H - 1 <= obj2.y + obj2.H - 1:
                return True
        return False

    def draw(self):
        if self.select.in_menu:
            self.select.draw()

        else:
            if self.animation_coffre:
                if self.etape_aniamtion_tresort < 3:
                    self.player.draw()
                    self.chest.draw(self.etape_aniamtion_tresort, self.player.orientation)
                else:
                    pyxel.cls(0)
                    pyxel.text(32, 62, "niv: " + str(self.map.niv + 1), 7)

            else:
                if self.map.fin:
                    pyxel.cls(0)
                    pyxel.text(8, 8, "Merci d'avoir joue", 7)
                elif self.map.niv == 4 and not self.dial_niv_5:
                    pyxel.cls(0)
                    pyxel.text(8, 8, "Prends garde aux mines.", 7)
                    pyxel.text(8, 16, "Elles ne sont visibles que", 7)
                    pyxel.text(8, 24, "sous l'eau, mais peuvent", 7)
                    pyxel.text(8, 32, "exploser dans les deux", 7),
                    pyxel.text(8, 40, "mondes. Bonne chance !", 7)
                elif not self.debut:
                    pyxel.cls(0)
                    self.map.draw()

                    self.les_entite.draw(self.player)
                    self.les_proj.draw(self.player.est_sous_l_eau)
                    self.player.draw()

                    if self.les_entite.mon_explo is not None:
                        self.les_entite.mon_explo.draw()

                    self.player.draw_barre_oxygene()
                    self.player.affiche_pv()
                    self.player.proj_count()
                    distance = pyxel.sqrt((self.player.x - self.chest.x) ** 2 + (self.player.y - self.chest.y) ** 2)
                    pyxel.text(0, 6, "Distance: " + str(int(distance)), 0)
                    pyxel.text(100, 4, 'niv:' + str(self.map.niv), 0)
                    if self.timer_jeu != None:
                        pyxel.text(0, 12, 'Temps: ' + self.format_temps(), 0)

                    if self.player.message is not None:
                        if pyxel.frame_count % 6 != 0:
                            pyxel.text(32, 62, self.player.message[0], 8)
                        self.player.message[1] += 1
                        if self.player.message[1] > 30:
                            self.player.message = None

                else:
                    pyxel.cls(0)
                    pyxel.text(8, 8, "Bonjour pirate. Tu es pret", 7)
                    pyxel.text(8, 16, "pour une chasse au tresor ?", 7)
                    pyxel.text(8, 24, "Le tresor est enfoui quelque", 7)
                    pyxel.text(8, 32, "part. Appuie sur D pour le", 7)
                    pyxel.text(8, 40, "deterrer. En appuyant sur C", 7)
                    pyxel.text(8, 48, "tu peux passer sous l'eau en", 7)
                    pyxel.text(8, 56, "2D. Certains elements", 7)
                    pyxel.text(8, 64, "disparaissent sous l'eau.", 7)
                    pyxel.text(8, 72, "profites-en pour passer .", 7)
                    pyxel.text(8, 80, "Bonne chance !", 7)

    def format_temps(self):
        temps_ecoule = pyxel.frame_count - self.timer_jeu
        min = temps_ecoule // 60 // 30
        temps_ecoule -= min * 1800
        return str(min) + ":" + str(temps_ecoule // 30)


Game()