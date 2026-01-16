from time import perf_counter
import sounddevice as sd
from amas.agent import Agent, NotWorkingError
from comprex.agent import ABEND, NEND, OBSERVER, READER, RECORDER, START
from comprex.audio import make_white_noise, Speaker
from comprex.config import Experimental
from comprex.scheduler import TrialIterator, blockwise_shuffle, unif_rng
from comprex.util import timestamp
from pino.ino import HIGH, LOW, Arduino

NOISE_IDX = 14
CONTROLER = "Controller"


async def present_stimulus(agent: Agent, ino: Arduino, pin: int,
                           duration: float) -> None:
    ino.digital_write(pin, HIGH)
    agent.send_to(RECORDER, timestamp(pin))
    await agent.sleep(duration)
    ino.digital_write(pin, LOW)
    agent.send_to(RECORDER, timestamp(-pin))
    return None


async def flush_message_for(agent: Agent, duration: float):
    while duration >= 0.:
        s = perf_counter()
        await agent.try_recv(duration)
        e = perf_counter()
        duration -= e - s


async def control(agent: Agent, ino: Arduino, expvars: Experimental) -> None:
    light_duration = expvars.get("light-duration", 1.)
    sound_duration = expvars.get("sound-duration", 1.)
    reward_duration = expvars.get("reward-duration", 0.05)

    light_pin = expvars.get("light-pin", [8, 9, 10, 11, 12])
    reward_pin = expvars.get("reward-pin", [6, 7])
    response_pins = expvars.get("response-pin", [2, 3])
    response_pins_str = list(map(str, response_pins))
    speaker = Speaker(expvars.get("speaker", 6))
    noise = make_white_noise(light_duration * 2.)  # Click音でも良い？

    mean_isi = expvars.get("inter-stimulus-interval", 19.)
    range_isi = expvars.get("interval-range", 10.)

    number_of_trial = expvars.get("number-of-trial", 200)
    isis = unif_rng(mean_isi, range_isi, number_of_trial)
    number_of_blocks = int(number_of_trial / (len(light_pin) * 2))
    light_order = blockwise_shuffle(light_pin * 2 * number_of_blocks,
                                    len(light_pin))
    stimulus_order = blockwise_shuffle(
        sum([[0, 1] for _ in range(5)], []) * number_of_blocks,
        len(light_pin) * 2)  # 0: light -> sound / 1: sound -> light
    trial_iterator = TrialIterator(list(range(number_of_trial)),
                                   list(zip(stimulus_order, light_order, isis)))

    try:
        while agent.working():
            agent.send_to(RECORDER, timestamp(START))
            for i, (is_light, light, isi) in trial_iterator:
                print(f"Trial {i}: Cue will be presented {isi} secs after.")
                # await agent.sleep(isi)
                await flush_message_for(agent, isi)
                if is_light:
                    agent.send_to(RECORDER, timestamp(light))
                    ino.digital_write(light, HIGH)
                    # await agent.sleep(light_duration)
                    await flush_message_for(agent, light_duration)
                    while agent.working():
                        _, response = await agent.recv()
                        if response == response_pins[1]:
                            break
                    agent.send_to(RECORDER, timestamp(-light))
                    ino.digital_write(light, LOW)
                    await present_stimulus(agent, ino, reward_pin[1],
                                           reward_duration)
                else:
                    agent.send_to(RECORDER, timestamp(NOISE_IDX))
                    speaker.play(noise, False, True)
                    # await agent.sleep(sound_duration)
                    await flush_message_for(agent, light_duration)
                    while agent.working():
                        _, response = await agent.recv()
                        if response == response_pins[0]:
                            break
                    agent.send_to(RECORDER, timestamp(-NOISE_IDX))
                    speaker.stop()
                    await present_stimulus(agent, ino, reward_pin[0],
                                           reward_duration)
            agent.send_to(OBSERVER, NEND)
            agent.send_to(RECORDER, timestamp(NEND))
            agent.finish()
    except NotWorkingError:
        agent.send_to(OBSERVER, ABEND)
        agent.send_to(RECORDER, timestamp(ABEND))
        agent.finish()
    return None


async def read(agent: Agent, ino: Arduino, expvars: Experimental):
    # read experimental variables from the given config file
    response_pin = expvars.get("response-pin", [9, 10])

    # cast to `str` to compare with inputs from Arduino
    response_pins_str = list(map(str, response_pin))

    # Reading inputs from Arduino
    try:
        while agent.working():
            input_: bytes = await agent.call_async(ino.read_until_eol)
            if input_ is None:
                continue
            parsed_input = input_.rstrip().decode("utf-8")
            agent.send_to(RECORDER, timestamp(parsed_input))
            if parsed_input in response_pins_str:
                agent.send_to(CONTROLER, parsed_input)

    except NotWorkingError:
        ino.cancel_read()


if __name__ == '__main__':
    from os import mkdir
    from os.path import exists, join

    from amas.connection import Register
    from amas.env import Environment
    from comprex.agent import Observer, Recorder, _self_terminate
    from comprex.config import PinoClap
    from comprex.util import get_current_file_abspath, namefile
    from pino.ino import Arduino, Comport

    config = PinoClap().config

    com = Comport() \
        .apply_settings(config.comport) \
        .set_timeout(1.0) \
        .deploy() \
        .connect()

    ino = Arduino(com)
    ino.apply_pinmode_settings(config.pinmode)

    data_dir = join(get_current_file_abspath(__file__), "data")
    if not exists(data_dir):
        mkdir(data_dir)
    filename = join(data_dir, namefile(config.metadata))

    controller = Agent("Controller") \
        .assign_task(control, ino=ino, expvars=config.experimental) \
        .assign_task(_self_terminate)

    # Use built-in agents
    reader = Agent(READER) \
        .assign_task(read, ino=ino, expvars=config.experimental) \
        .assign_task(_self_terminate)

    recorder = Recorder(filename=filename)
    observer = Observer()

    agents = [controller, reader, recorder, observer]
    register = Register(agents)
    env = Environment(agents)

    try:
        env.run()
    except KeyboardInterrupt:
        observer.send_all(ABEND)
        observer.finish()