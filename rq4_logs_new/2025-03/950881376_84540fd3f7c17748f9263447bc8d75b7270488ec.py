import numpy as np
import customtkinter  as ctk
import os

from components.create_frame import CreateFrame
from components.create_scrollable_frame import CreateScrollableFrame
from components.grid_frame import GridFrame
from components.thread_camera import ThreadCamera
from components.frame_navbar import FrameNavbar
from pages.parameter_checking.frame_parameter_checking import FrameParameterChecking
from pages.calibration.frame_calibration import FrameCalibration
from pages.estimation.frame_estimation import FrameEstimation
from pages.exercise_routine.frame_exercise_routine import FrameExerciseRoutine
from pages.exercise_routine.frame_exercise_classification import FrameExerciseClassification
            
"""
Frame principal de la aplicacion.
"""
class FrameApplication(CreateFrame):
    def __init__(self, master, name, **kwargs):
        CreateFrame.__init__(self, master=master, name=name, grid_frame=GridFrame(dim=(1,8), arr=np.array([["0,0","0,1","0,1","0,1","0,1","0,1","0,1","0,1"]])), **kwargs)

        # Directorio de proyecto
        self.current_path=os.path.dirname(os.path.abspath('__file__')) 
        self.folder_name_algorithm_images=os.path.join(self.current_path, *["pinhole_camera_model", "algorithms", "algorithm_images"])
        self.folder_name_optimal_parameters=os.path.join(self.current_path, *["pinhole_camera_model", "optimal_parameters"])
        self.folder_name_data=os.path.join(self.current_path, *["data"])

        # Inicializacion de los hilos (threads) de las camaras
        self.thread_camera_1=ThreadCamera(root_name="camera_1", folder_name_optimal_parameters=self.folder_name_optimal_parameters)
        self.thread_camera_2=ThreadCamera(root_name="camera_2", folder_name_optimal_parameters=self.folder_name_optimal_parameters)
        self.thread_camera_1.start()
        self.thread_camera_2.start()
        self.frame_navbar=None

        # Frame contenedor
        self.frame_container=CreateScrollableFrame(master=self, grid_frame=GridFrame(dim=(5,1), arr=np.array([["0,0"],["1,0"],["1,0"],["1,0"],["1,0"]])), orientation="vertical")
        # Frame de navegacion
        self.frame_navbar=FrameNavbar(master=self.frame_container, name="FrameNavbar", fg_color="lightcoral")
        # Frame de clasificacion de ejercicio
        self.frame_exercise_classification=FrameExerciseClassification(master=self.frame_container, name="FrameExerciseClassification", fg_color="lightcoral")
        self.frame_container.insert_element(cad_pos="0,0", element=self.frame_navbar, padx=5, pady=5, sticky="nsew")
        self.frame_container.insert_element(cad_pos="1,0", element=self.frame_exercise_classification, padx=5, pady=5, sticky="nsew")

        self.insert_element(cad_pos="0,0", element=self.frame_container, padx=5, pady=5, sticky="nsew")

    # Para establecer la pagina de inicio
    def set_page(self, page_name):
        self.destroy_element(cad_pos="0,1")

        frame_page=None
        if page_name == "parameter_checking":
            frame_page=FrameParameterChecking(master=self, name="FrameParameterChecking", fg_color="lightcoral", orientation="vertical")    
        elif page_name == "calibration":
            frame_page=FrameCalibration(master=self, name="FrameCalibration", fg_color="lightcoral", orientation="vertical") 
        elif page_name == "estimation":
            frame_page=FrameEstimation(master=self, name="FrameEstimation", callback_estimations=self.callback_estimations, fg_color="lightcoral", orientation="vertical") 
        elif page_name == "exercise_routine":
            frame_page=FrameExerciseRoutine(master=self, name="FrameExerciseRoutine", fg_color="lightcoral", orientation="vertical") 

        # Notificamos a los oyentes de los cambios en las camaras 
        self.thread_camera_1.notify_frame_listeners()
        self.thread_camera_2.notify_frame_listeners()

        self.insert_element(cad_pos="0,1", element=frame_page, padx=5, pady=5, sticky="nsew")

    # Para recibir los algoritmos actuales y sus correspondientes datos
    def callback_estimations(self, algorithm):
        self.frame_exercise_classification.process_data(algorithm=algorithm)

"""
Clase para iniciar la aplicacion de CustomTkinter.
"""
class App(ctk.CTk):
    def __init__(self):
        ctk.CTk.__init__(self)

        # Configuramos nuestra aplicacion
        self.geometry("1366x768")
        self.title("Physical Therapy Project")

        # Configuramos el sistema de cuadricula
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # Creamos un frame root
        self.frame_root=CreateFrame(master=self, name="FrameRoot", grid_frame=GridFrame(dim=(1,1), arr=None))
        
        # Colocamos el frame root en la cuadricula
        self.frame_root.grid(row=0, column=0, padx=5, pady=5, sticky="nsew") # Al agregar sticky='nsew' el frame pasa de widthxheight a abarcar todo el espacio disponible

        # Creamos el elemento principal 
        self.frame_application=FrameApplication(master=self.frame_root, name="FrameApplication")

        # Insertamos el elemento principal
        self.frame_root.insert_element(cad_pos="0,0", element=self.frame_application, padx=5, pady=5, sticky="nsew")

        # Configuramos el cerrado de la ventana
        self.protocol("WM_DELETE_WINDOW", self.destroy)

    def destroy(self):
        ctk.CTk.quit(self)
        ctk.CTk.destroy(self)

if __name__ == "__main__":
    # Configuramos e iniciamos la aplicacion
    ctk.set_appearance_mode("Light")
    ctk.set_default_color_theme("green")
    app=App()
    app.mainloop()