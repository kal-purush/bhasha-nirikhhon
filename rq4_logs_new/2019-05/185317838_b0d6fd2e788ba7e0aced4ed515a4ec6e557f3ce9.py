from multiprocessing import Process

import anki_vector


def playMusic():
    args = anki_vector.util.parse_command_args()
    with anki_vector.Robot(args.serial) as robot:
        robot.audio.stream_wav_file("rickroll.wav", 50)


def dance():
    args = anki_vector.util.parse_command_args()
    with anki_vector.Robot(args.serial) as robot:
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')
        robot.anim.play_animation('anim_dancebeat_scoot_left_01')
        robot.anim.play_animation('anim_dancebeat_scoot_right_01')
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')
        robot.anim.play_animation('anim_dancebeat_scoot_left_01')
        robot.anim.play_animation('anim_dancebeat_scoot_right_01')
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')
        robot.anim.play_animation('anim_dancebeat_scoot_left_01')
        robot.anim.play_animation('anim_dancebeat_scoot_right_01')
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')
        robot.anim.play_animation('anim_dancebeat_scoot_left_01')
        robot.anim.play_animation('anim_dancebeat_scoot_right_01')
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')
        robot.anim.play_animation('anim_dancebeat_scoot_left_01')
        robot.anim.play_animation('anim_dancebeat_scoot_right_01')
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')
        robot.anim.play_animation('anim_dancebeat_scoot_left_01')
        robot.anim.play_animation('anim_dancebeat_scoot_right_01')
        robot.anim.play_animation('anim_dancebeat_pivot_left_01')
        robot.anim.play_animation('anim_dancebeat_pivot_right_01')


def main():
    args = anki_vector.util.parse_command_args()
    with anki_vector.Robot(args.serial) as robot:
        robot.behavior.drive_off_charger()

        p1 = Process(target=playMusic)
        p1.start()
        p2 = Process(target=dance)
        p2.start()
        p1.join()
        p2.join()


if __name__ == "__main__":
    main()