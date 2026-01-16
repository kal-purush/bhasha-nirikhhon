# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 13:14:09 2024

@author: jonas.lenz
"""
import streamlit as st
import pandas as pd
import numpy as np
import os
import copy


def _select_project(projectlist):
    project = st.radio("Select your Project",
                       options = [x for x in projectlist],
                       format_func = lambda x: projectlist[x],
                       index = None
                       )
    return project

def _update_agrovoc_concept_dump():
    """Get the list of all concepts of agrovoc to a local dict and pickle this.

    Agrovoc currently includes < 44000 concepts, so limit of 100000 concepts
    is sufficient.
    the python sparql wrapper is described there:
        https://sparqlwrapper.readthedocs.io/en/latest/main.html#command-line-script
    the agrovoc sparql endpoint with examples there:
        https://agrovoc.fao.org/sparql
    I build upon the example while deleting date infos:
        "Select all concepts added since date X (e.g. 31/12/2020), URI and EN prefLabel."
    """
    from SPARQLWrapper import SPARQLWrapper, JSON
    import pickle

    sparql = SPARQLWrapper(
        "https://agrovoc.fao.org/sparql/"
    )
    sparql.setReturnFormat(JSON)

    sparql.setQuery("""
                    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                    PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
                    SELECT ?concept ?label
                    WHERE {
                        ?concept a skos:Concept .
                        OPTIONAL {
                            ?concept skosxl:prefLabel ?xEnLabel .
                            ?xEnLabel skosxl:literalForm ?label .
                            }
                        FILTER(lang(?label) = 'en')
                        }
                    LIMIT 100000
                    """
                    )
    ret = sparql.queryAndConvert()
    with open("agrov.dump", 'wb') as handle:
        pickle.dump(ret, handle, protocol=pickle.HIGHEST_PROTOCOL)




def _get_agrovoc_dump():
    import pickle

    if not os.path.isfile("agrov.dump"):
        _update_agrovoc_concept_dump()
    with open("agrov.dump", 'rb') as handle:
        ret = pickle.load(handle)
    optio = {r["label"]["value"]:
             r["concept"] for r in ret["results"]["bindings"]}
    return optio



def _modify_agrovoc_concept(container):
    optio = _get_agrovoc_dump()
    keys = list(optio.keys())
    if "agrovoc" in container:
        index = keys.index("soil organic matter")
    else:
        index = None
    agrovoc = st.selectbox("AGROVOC label",
                           keys,
                           help="Agrovoc is a controled vocabulary.\
                               By assigning these vocabularies to your\
                               data, a common understanding of the meaning\
                               can be achieved.",
                           index=index,
                           placeholder="Choose an AGROVOC concept.")
    if agrovoc:
        st.write(agrovoc)
        st.write(
            "https://agrovoc.fao.org/browse/agrovoc/en/page/?uri=" +
            optio[agrovoc]["value"])


def _mod_container_content(container_org):
    modc1, modc2 = st.columns(2)
    container_mod = copy.deepcopy(container_org)
    if container_mod.containerType == 'json':
        attributes = ['containerType', 'content', 'name']
    elif container_mod.containerType == 'file':
        attributes = ['encoding', 'metadataElements', 'name']
    elif container_mod.containerType == 'directory':
        attributes = ['parentContainer', 'containers', 'name']
    else:
        attributes = [method_name for method_name in dir(container_mod)
                          if not callable(getattr(container_mod, method_name))
                          and not "__" in method_name]

    for attribute in attributes:
## json error
        if attribute == 'scontent':
            with modc1:
                st.json(getattr(container_mod, attribute))
        else:
            with modc1:
                setattr(container_mod, attribute, st.text_input(
                        label = attribute,
                        value = str(getattr(container_mod, attribute))
                        )
                    )
            with modc2:
                test = st.button("Test Changes for "+attribute,
                           disabled = getattr(container_mod, attribute) == getattr(container_org, attribute))

                change = st.button("Save Changes for "+attribute,
                           disabled = getattr(container_mod, attribute) == getattr(container_org, attribute))
        if change:
            change = False
            setattr(container_org, attribute, getattr(container_mod, attribute))
            st.rerun()
    if test:
        return container_mod
    else:
        return container_org

def _get_data_for_concept(container, agrovoc):
    # queries actual and all subcontainers for data of controlled vocabularies and their main identifier
    if container == 1:
        data = pd.DataFrame([[2],
                             [1.5],
                             [2.3],
                             [-0.5],
                             [1]],
                            columns=[str(container)])
    else:
        data = pd.DataFrame(np.random.randn(5, 1),
                            columns=[str(container)])
    return data

def _visualize_data(container, mainID, agrovoc, projects=[]):
    chart_data = _get_data_for_concept(1, "Runoff")
    for x in projects:
        chart_data[str(x)] = _get_data_for_concept(x, "Runoff")[str(x)]
    # chart_data is data got from container
    st.write(chart_data)
    st.write("showing mockup data")
    st.line_chart(chart_data)
    pass