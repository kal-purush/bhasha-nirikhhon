import multiplication_table as mt
import time
from tkinter import Tk, Canvas


# Représentation fixe


# Example 1 :
start = time.time()
root = Tk()
canvas = Canvas(root, width=750, height=750, bg='white')
canvas.pack(side="left", fill="both", expand=True)
mt.circle(canvas=canvas, center=(360, 360), radius=300,
          state_circle=True, background_circle="", outline_circle="black")
mt.dot(canvas=canvas, graph=mt.Graph(2, 10), radius=300, center=360,
       color_graph=['black', 'purple', "blue", "red", "cyan"],
       color_name="red")
mt.all_edges(canvas=canvas, graph=mt.Graph(2, 10), radius=300, center=360,
             color_graph=['black', 'purple', "blue", "red", "cyan"],
             edges_width=2)
root.mainloop()
end = time.time()
print("Fixe representation 'time : " + str(end-start))


# Example 2 :
start = time.time()
root = Tk()
canvas = Canvas(root, width=750, height=750, bg='orange')
canvas.pack(side="left", fill="both", expand=True)
mt.circle(canvas=canvas, center=(360, 360), radius=300,
          state_circle=True, background_circle="red", outline_circle="black")
mt.dot(canvas=canvas, graph=mt.Graph(2, 10), radius=300, center=360,
       color_graph=["blue", "cyan"],
       color_name="yellow")
mt.all_edges(canvas=canvas, graph=mt.Graph(2, 10), radius=300, center=360,
             color_graph=[ "blue", "cyan"],
             edges_width=2)
root.mainloop()
end = time.time()
print("Fixe representation 'time : " + str(end-start))