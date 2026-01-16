
from collections import defaultdict
from os import path

import sh
from django.conf import settings  # database dir
from django.core.management import call_command  # call newPR update command
from django.core.management.base import BaseCommand
from funcaddPRinfo import addPRinfo
from funcaddScreenshots import addScreenshots

KOLIBRI_DIR = path.abspath(settings.KOLIBRI_DIR)


def switchBranch(gitBranch):
    kolibri_git_dir = path.abspath(path.join(KOLIBRI_DIR, '.git'))

    sh.git('--git-dir', kolibri_git_dir, '--work-tree',
           KOLIBRI_DIR, 'checkout', gitBranch)


class Command(BaseCommand):

    def add_arguments(self, parser):
        # the Pull Request Number to search for on git API
        parser.add_argument('prNumber', type=int)

    def handle(self, *args, **options):

        # output the states that were added to the database
        savedStatesDict = addPRinfo(options['prNumber'])
        screenshotConfig = settings.SCREENSHOT_CONFIG

        newImgDict = defaultdict(list)  # {'key': [ImgObj1>, <ImgObj2>], 'key2': [ImgObj1>, ...>]}
        for stateName in savedStatesDict:
            for state in savedStatesDict[stateName]:  # should only be two
                switchBranch(state.gitBranch)
                imgList = addScreenshots(state, screenshotConfig)
                for i in imgList:
                    # key should proabbly also use stateName, but stateNames needs not change
                    key = "{0}{1}{2}{3}x{4}".format(i.state.stateUrl,
                                                    i.browserType,
                                                    i.operatingSystem,  # {2}
                                                    i.width,
                                                    i.height)
                    newImgDict[key].append(i)

        for imgPair in newImgDict:
            call_command('addImgDiff',
                         'imagemagick',
                         newImgDict[imgPair][0].imgName,
                         newImgDict[imgPair][1].imgName)