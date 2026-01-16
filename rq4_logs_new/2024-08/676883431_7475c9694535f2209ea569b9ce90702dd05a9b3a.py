# -*- coding: utf-8 -*-
"""
@author: Jan Devátý, Jonas Lenz
"""

from soilpulse.project_management import *
from src.soilpulse.resource_managers.filesystem import *
from src.soilpulse.resource_managers.mysql import *
from src.soilpulse.resource_managers.xml import *
from src.soilpulse.resource_managers.json import *
from src.soilpulse.data_publishers import *
from src.soilpulse.metadata_scheme import *
from src.soilpulse.db_access import EntityKeywordsDB, DBconnector

def establish_new_project(user_id, **example):
    """
    use case function
    """
    print("\n\n" + 150 * "#")
    print("CREATE NEW PROJECT")
    print("\n".join([f"{k}: {v}" for k, v in example.items()]))
    print(150 * "#"+"\n")
    example.update({"user_id": user_id})

    # create ResourceManager instance for new resource:
    try:
        project = ProjectManager(**example)

    except DatabaseEntryError as e:
        # this exception is thrown when trying to add new ResourceManager with existing name into the database (for same user)
        # pass the error message to the user ... some pop-up window with the message
        print(e.message)
        pass
    except NotImplementedError:
        print(
            f"Publisher of requested DOI record related files 'is not supported.\nCurrently implemented publishers: {[', '.join([k for k in PublisherFactory.publishers.keys()])]}")

    else:
        # download files associated with the publisher record
        try:
            project.downloadPublishedFiles()
        except DOIdataRetrievalException as e:
            print(f"Files of DOI record couldn't be downloaded due to DOI data response error.\n{e.message}")

        # setting of files 'licensing' - this property should be available through GUI
        project.keepFiles = True

        # show the whole container tree
        project.showContainerTree()

        # new empty dataset is created and added to the ResourceManager
        newDataset = project.newDataset("Dataset test 1")
        # add some containers from the ResourceManager - will be done through the GUI
        newDataset.addContainers(project.getContainerByID([1, 2, 6]))

        # # show the dataset's container tree
        # newDataset.showContainerTree()
        # newDataset.getCrawled()

        project.updateDBrecord()

    return project

def load_existing_project(user_id, project_id):
    """
    use case function
    """

    print("\n\n" + 150 * "#")
    print("LOAD EXISTING PROJECT")
    print(f"user_id: {user_id}\nproject_id: {project_id}")
    print(150 * "#"+"\n")

    example = {"user_id": user_id, "id" : project_id}
    # create ProjectManager instance for loaded resource:
    try:
        project = ProjectManager(**example)

    except DatabaseEntryError as e:
        # this exception is thrown whne trying to add new ProjectManager with same name into the database (for same user)
        # pass the error message to the user ... some pop-up window with the message
        print(e.message)
        pass
    except NotImplementedError:
        print(
            f"Publisher of requested DOI record related files 'is not supported.\nCurrently implemented publishers: {[', '.join([k for k in PublisherFactory.publishers.keys()])]}")

    else:

        # show the whole container tree
        project.showContainerTree()

        #show paths of files and related containers
        project.showFilesStructure()
        # # change Resource name ... testing
        # project.name = "Jonas' dissertation"
        # project.updateDBrecord()

    return project


# example DOI records that can be used
example_1 = {"name": "Jonas Lenz's dissertation package", "doi": "10.5281/zenodo.6654150"}
example_2 = {"name": "", "doi": "10.5281/zenodo.6654150"}
example_3 = {"name": "Michael Schmuker's neuromorphic_classifiers", "doi": "10.5281/zenodo.18726"}  # more lightweight repo
example_4 = {"name": "Ries et al.", "doi": "10.6094/unifr/151460"}


def test_create_project(**example1):

    # user identifier that will be later managed by some login framework in streamlit
    # it's needed for loading ProjectManagers from database - user can access only own resources
    user_id = 1

    project1 = establish_new_project(user_id, **example_1)
    
    assert project1.doi == "10.5281/zenodo.6654150"


def notest_create_project2(example4):
    
    # user identifier that will be later managed by some login framework in streamlit
    # it's needed for loading ProjectManagers from database - user can access only own resources
    user_id = 1

    project1 = establish_new_project(user_id, **example_1)
    
    assert project1.doi == "10.6094/unifr/151460"

def notest_load_project(example1):

    user_id = 1
    project1 = load_existing_project(user_id, 2)
    
    assert project1.doi == "10.5281/zenodo.6654150"
