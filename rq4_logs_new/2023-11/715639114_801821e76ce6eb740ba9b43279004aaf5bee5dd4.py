#!/usr/bin/env python3

import rclpy
from rclpy.node import Node

from math import sqrt, acos, pi, atan2
import matplotlib.pyplot as plt

from sensor_msgs.msg import NavSatFix, Imu
from std_msgs.msg import Float64
from geometry_msgs.msg import Point

# calcul de la norme d'un vecteur
def norm(v):
    return sqrt(v[0]**2 + v[1]**2)

# calcul du produit scalaire de deux vecteurs
def dotprod(v, u):
    return v[0] * u[0] + v[1] * u[1]


# calcul de la soustraction du vecteur v par le vecteur u
def vectsub(v, u):
    return(v[0] - u[0], v[1] - u[1])

# renvoie 1 si v est à gauche de u, -1 si à droite
def get_side(v, u):
    dot = u[0] * (-v[1]) + u[1] * v[0]
    if dot > 0:
        return 1
    else:
        return -1
# renvoie l'angle entre le vecteur v et le vecteur u (de -pi à pi)    
def get_angle(v, u):
    if norm(v) == 0 or norm(u) == 0:
        return 0
    else :
        return get_side(v, u) * acos( dotprod(v, u) / (norm(v) * norm(u)) )

class PID(Node):
    def __init__(self):
        super().__init__("pid")

        # Initialisation des tableaux pour la visualisation à la fin (debug)
        self.angles = []
        self.turn   = []
        self.times  = []
        self.Ps     = []
        self.Ds     = []
        self.Is     = []

        # initialisation des variables

        # ------ Variables du PID, c'est celle-là qu'il faut modifier pour changer la réponse du bateau -------
        self.P              = 0.75                                # Facteur de l'erreur de position   
        self.I              = 0.00015                             # Facteur de l'erreur d'intégrale
        self.D              = 300000000                           # Facteur de l'erreur de dérivé


        self.pos            = (0.0, 0.0)                          # Vecteur de la position actuelle
        self.orientation    = 0.0                                 # Vecteur de l'orientation du bateau
        self.coord_objectif = (0.0, 0.0)                          # Vecteur de la position de l'objectif
        self.eps_obj        = 0.00000001                          # Valeur petite pour laquelle on considère que les coordonnées d'objectif ont changées
        self.eps_arrived    = 0.0001                              # Valeur petite pour laquelle on condidère qu'on est assez proche de l'arrivé pour être arrivé
                                  
        self.olderr         = 0.0                                 # Valeur de l'erreur au pas précédent, pour le calcul de la dérivée
        self.integ_err      = 0.0                                 # Valeur de l'erreur d'integrale
        self.d_err          = 0.0                                 # Valeur de l'erreur de dérivée
        self.time           = self.get_clock().now().nanoseconds  # Temps pour calculer les dérivés
        self.t              = 0                                   # Valeur pour l'affichage à la fin (debug)


        # initialisation des publisher pour les mouvements du bateau
        self.left_speed_pub  = self.create_publisher(Float64, '/wamv/thrusters/left/thrust', 5)
        self.right_speed_pub = self.create_publisher(Float64, '/wamv/thrusters/right/thrust', 5)
        self.left_turn_pub   = self.create_publisher(Float64, '/wamv/thrusters/left/pos', 5)
        self.right_turn_pub  = self.create_publisher(Float64, '/wamv/thrusters/right/pos', 5)
        self.main_turn_pub   = self.create_publisher(Float64, '/wamv/thrusters/main/pos', 5)
        self.main_speed_pub  = self.create_publisher(Float64, '/wamv/thrusters/main/thrust', 5)


        # initialisation des subscription pour récupérer les infos utiles sur le bateau
        self.gps = self.create_subscription(NavSatFix, '/wamv/sensors/gps/gps/fix', self.gps_gatherer, 10)
        self.imu = self.create_subscription(Imu, '/wamv/sensors/imu/imu/data', self.imu_gatherer, 10)

        # obtention de la position voulue
        self.objectif = self.create_subscription(Point, 'team52/waypoint', self.objectif_gatherer, 10)

        # lancement de la fonction principale
        self.run()

        
    # fonctions pour les subscribers


    def gps_gatherer(self, msg):
        # On récupère les positions
        self.pos = (msg.longitude, msg.latitude)

    def imu_gatherer(self, msg):
        q = (msg.orientation.x, msg.orientation.y, msg.orientation.z, msg.orientation.w)
        self.orientation = atan2(2 * (q[3] * q[2] + q[0] * q[1]), 1 - 2 * (q[0]**2 + q[3]**2))
        
    def objectif_gatherer(self, msg):
        # Si les valeurs du messages sont significativement différentes des valeurs actuelles
        if abs(self.coord_objectif[0] - msg.x.data) > self.eps_obj or abs(self.coord_objectif[1] - msg.y.data) > self.eps_obj:
            # On change l'objectif
            self.coord_objectif = (msg.x.data, msg.y.data)

    # fonctions temporaires avant de savoir comment fonctionnent les types msg
    def objectif_gatherer_x(self, msg):
        # Si les valeurs du messages sont significativement différentes des valeurs actuelles
        if abs(self.coord_objectif[0] - msg.data) > self.eps_obj:
            # On change l'objectif
            self.coord_objectif = (msg.data, self.coord_objectif[1])
    
    def objectif_gatherer_y(self, msg):
        # Si les valeurs du messages sont significativement différentes des valeurs actuelles
        if abs(self.coord_objectif[1] - msg.data) > self.eps_obj:
            # On change l'objectif
            self.coord_objectif = (self.coord_objectif[0], msg.data)


    # fin des fonctions pour les subscribers


    # fonction qui calcule les erreurs, et renvoie l'angle du gouvernail
    def new_get_steering_angle(self):
        # On récupère le vecteur qui pointe du bateau vers l'objectif
        obj = vectsub(self.coord_objectif, self.pos)
        # On calcul l'angle qu'il faut pour pointer vers cet objectif
        obj_angle = get_angle(obj, (-1.0, 0.0))
        print(obj_angle)
        # On calcul l'angle qu'il y a entre l'objectif et l'orientation du bateau
        angle = self.orientation - obj_angle

        if angle > pi:
            angle -= 2 * pi
        elif angle < -pi:
            angle += 2 * pi
            
        # On append pour l'affichage à la fin (debug)
        self.angles.append(-angle)
        self.times.append(self.t)

        # On calcul l'erreur de position
        angle_error = - angle

        # On calcul la dérivée
        # On attend de voir si la valeur de l'angle à été mis à jour (sinon la dérivé serait nulle)
        if self.olderr != angle_error:
            # On calcul la dérivé 
            self.d_err = (angle_error - self.olderr) / (self.get_clock().now().nanoseconds - self.time)
            # On met à jour l'ancienne valeur d'angle
            self.olderr = angle_error
            # On recalcul le temps pour la futur dérivé
            self.time = self.get_clock().now().nanoseconds

        # borner les valeurs de la dérivée
        Kd = self.D * self.d_err
        if Kd > 7:
            Kd = 7
        elif Kd < -7:
            Kd = -7


        # On calcul l'intégrale (pas testé)
        self.integ_err += angle_error

        # On calcul la sortie, avec le PID (pour l'instant PD)
        steering_angle = self.P * angle_error + Kd + self.I * self.integ_err
        self.Ps.append(self.P * angle_error)
        self.Ds.append(Kd)
        self.Is.append(self.I * self.integ_err)

        return steering_angle


    # fonction principale
    def run(self):
        # Si on est déjà arrivé, on bouge pas, sinon on trace
        if norm(vectsub(self.pos, self.coord_objectif)) < self.eps_arrived:
            speed = 0.0
        else:
            speed = 12000.0
        
        # L'angle du gouvernail est initialement à 0
        angle = 0.0
        # L'angle max du gouvernail est de pi/4 et -pi/4
        turn_limit = 0.78539816339

        try:
            self.get_logger().info("running")
            while rclpy.ok():
                # On incrémente t pour l'affichage des données à la fin (debug)
                self.t += 1

                # Si on est suffisament proche de l'arrivé, on s'arrête
                #print(norm(vectsub(self.pos, self.coord_objectif)))
                if norm(vectsub(self.pos, self.coord_objectif)) < self.eps_arrived:
                    speed = 0.0
                else:
                    speed = 12000.0
        
                # On récupère l'angle du gouvernail
                angle = self.new_get_steering_angle()

                # On borne l'angle possible
                if angle < -turn_limit:
                    angle = -turn_limit
                elif angle > turn_limit:
                    angle = turn_limit

                # On met les donnés en forme pour publish
                self.turn.append(angle)
                speed_msg = Float64()
                angle_msg = Float64()
                speed_msg.data = speed
                angle_msg.data = angle

                # On publish
                self.left_speed_pub.publish(speed_msg)
                self.right_speed_pub.publish(speed_msg)
                self.left_turn_pub.publish(angle_msg)
                self.right_turn_pub.publish(angle_msg)
                self.main_turn_pub.publish(angle_msg)
                self.main_speed_pub.publish(speed_msg)

                # On spin le node pour recommencer
                rclpy.spin_once(self)

        except Exception as e:
            print(e)

        finally:
            # On affiche l'angle du bateau par rapport au vecteur qui pointe vers les coordonnées objectif, en fonction du temps
            plt.plot(self.Ds, self.times, color='g')
            plt.plot(self.Is, self.times, color='b')
            plt.plot(self.Ps, self.times, color='r')
            plt.plot(self.angles, self.times, color='y')

            plt.show()

            # l'idée ici c'est que si le progamme s'arrête, on demande à ce que le bateau s'arrête
            # Mais ca marche pas
            msg = Float64()
            msg.data = 0.0
            self.left_speed_pub.publish(msg)
            self.right_speed_pub.publish(msg)
            self.left_turn_pub.publish(msg)
            self.right_turn_pub.publish(msg)
            self.main_turn_pub.publish(msg)
            self.main_speed_pub.publish(msg)


        
def main(args=None):
    rclpy.init(args=args)
    
    subscriber = PID()
    #rclpy.spin(subscriber)


    subscriber.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()