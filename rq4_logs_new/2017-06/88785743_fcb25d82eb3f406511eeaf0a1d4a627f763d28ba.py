'''
GOAL: high level command that Diffs two images and add to DIFF table
given: two images to diff, diff method of choice
return: diff image between 2 screenshots of a state,
        add a new diff object to DIFF table
'''
import os
from StringIO import StringIO

from django.conf import settings  # database dir
from django.core.management import call_command  # call genDiff command
from django.core.management.base import BaseCommand, CommandError
from HookCatcher.models import Diff, Image

# directory for storing images in the data folder
IMG_DATABASE_DIR = os.path.join(settings.DATABASE_DIR, 'img')


def getDiffImageName(imgObj1, imgObj2):
    # format the name of the diff image to be base(target) commit -> diffs -> head(source) commit

    # formula: (baseGitinfo)/diffs/(headGitInfo)/(imgMetadata).png
    stateAndRepoBase = os.path.join(imgObj1.state.stateName, imgObj1.state.gitRepo)
    branchAndCommitBase = os.path.join(imgObj1.state.gitBranch, imgObj1.state.gitCommit.gitHash[:7])
    imgPathBase = os.path.join(stateAndRepoBase, branchAndCommitBase)

    stateAndRepoHead = os.path.join(imgObj2.state.stateName, imgObj2.state.gitRepo)
    branchAndCommitHead = os.path.join(imgObj2.state.gitBranch, imgObj2.state.gitCommit.gitHash[:7])
    imgPathHead = os.path.join(stateAndRepoHead, branchAndCommitHead)

    diffPath = os.path.join(os.path.join(imgPathBase, 'diffs'), imgPathHead)
    name = '{0}_{1}_{2}x{3}.png'.format(imgObj1.browserType,  # {0}
                                        imgObj1.operatingSystem,
                                        imgObj1.width,    # {2}
                                        imgObj1.height)

    diffCompletePath = os.path.join(diffPath, name)
    print diffCompletePath
    return diffCompletePath


def addDiffData(diffImgName, imgObj1, imgObj2, diffPercent):
    # check if image already exists in data to prevent duplicates
    findDuplicateDiff = Diff.objects.filter(targetImg=imgObj1,
                                            sourceImg=imgObj2)

    # if there was no duplicate found
    if (findDuplicateDiff.count() > 0):
        findDuplicateDiff = findDuplicateDiff.get()
        print('A diff between these two images already exists in the database')
        user_input = raw_input("Overwrite the image? (y/n): ")
        if(user_input == 'y'):
            print('Overwriting...')
            findDuplicateDiff.diffImgName = diffImgName
            return findDuplicateDiff
        else:
            return

    else:
        diffObj = Diff(diffImgName=diffImgName,
                       targetImg=imgObj1,
                       sourceImg=imgObj2,
                       diffPercent=diffPercent)
        diffObj.save()
        return diffObj


class Command(BaseCommand):
    help = 'Choose two image screenshots of the same state, resolution, os, and browser to take a diff of'  # noqa: E501

    def add_arguments(self, parser):
        # use state UUID for identification rather thatn commitHash, repo, branch, state names
        parser.add_argument('diffTool')
        parser.add_argument('imgName1')
        parser.add_argument('imgName2')

# if one of the image objects don't exist in database should we just add it?
    def handle(self, *args, **options):
        try:
            # Make sure these images exist in the image database
            diffTool = options['diffTool']
            img1 = Image.objects.get(imgName=options['imgName1'])
            img2 = Image.objects.get(imgName=options['imgName2'])
        except:
            raise CommandError('At least one of the two images does not exist in the database')

        imgPath1 = os.path.join(IMG_DATABASE_DIR, img1.imgName)
        imgPath2 = os.path.join(IMG_DATABASE_DIR, img2.imgName)
        diffName = os.path.join(IMG_DATABASE_DIR, getDiffImageName(img1, img2))

        out = StringIO()
        call_command('genDiff', diffTool, imgPath1, imgPath2, diffName, stdout=out)

        percentDiff = float(out.getvalue())
        newDiff = addDiffData(diffName, img1, img2, percentDiff)

        if(newDiff):
            self.stdout.write(self.style.SUCCESS('Finished adding Diff: %s' % newDiff))
        else:
            raise CommandError('No new diff was added')