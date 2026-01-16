import carla
from random import choice
from time import sleep
from numpy import array
from cv2 import imshow, waitKey

def cam_image(image):
    shape = (480, 640, 4)
    i = array(image.raw_data).reshape(shape)[:, :, :3]
    imshow("", i)
    waitKey(1)

client = carla.Client("localhost", 2000)
client.set_timeout(2.0)

world = client.get_world()
if world.get_map().name != "Town02":
    world = client.load_world("Town02")

# Escolhendo a posição onde ele será colocado
spawn_point = choice(world.get_map().get_spawn_points())

# pegando a biblioteca blueprint
blueprint_library = world.get_blueprint_library()

# Acoplando uma câmera
camera_rgb = blueprint_library.find("sensor.camera.rgb")
camera_rgb.set_attribute("image_size_x", "640")
camera_rgb.set_attribute("image_size_y", "480")
camera_rgb.set_attribute("fov", "110")

# Escolhendo o veículo
while True:
    vehicle = choice(blueprint_library.filter("vehicle"))
    if int(vehicle.get_attribute("number_of_wheels")) == 4:
        break
    
vehicle.set_attribute("role_name", "autopilot")
if vehicle.has_attribute("color"):
    vehicle.set_attribute("color", choice(vehicle.get_attribute("color").recommended_values))

env = carla.command.SpawnActor(vehicle, spawn_point).then(carla.command.SetAutopilot(carla.command.FutureActor, True))
actor_id = client.apply_batch_sync([env])[0].actor_id
actor = world.get_actors([actor_id])[0]

loc = actor.get_location()
trans_loc = carla.Transform(
        carla.Location(
            x=loc.x,
            y=loc.y,
            z=loc.z)
        )

cam = world.spawn_actor(
    camera_rgb,
    trans_loc,
    attach_to=actor
    )
cam.listen(lambda data: cam_image(data))

sleep(20)
actor.destroy()
cam.destroy()
