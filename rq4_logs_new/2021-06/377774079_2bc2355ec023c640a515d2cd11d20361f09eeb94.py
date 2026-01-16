from lib.afreeca_divider import extractor
from lib.twitch_compensator import compensator
from lib.folder_handler import call_file_list, file_remover
from multiprocessing import Pool


def main(platform):
    folder_dir = f'C:\\Users\\whiletrue2\\Desktop\\stream-collector\\streamlink\\videos\\{platform}'

    for file in call_file_list(folder_dir):
        try:
            if platform == 'afreeca':
                extractor(folder_dir, file)
            elif platform == 'twitch':
                compensator(folder_dir, file)
        except Exception as e:
            print(f'error {e}')
        else:
            file_remover(folder_dir, file)


def run():
    with Pool(processes=2) as pool:
        platform_list = ['afreeca', 'twitch']
        pool.map(main, platform_list)


if __name__ == '__main__':
    run()