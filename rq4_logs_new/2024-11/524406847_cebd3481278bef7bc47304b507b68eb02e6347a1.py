def output(fem,output_file):

    with open(output_file,"w") as f:
        ### Header ###
        f.write("# vtk DataFile Version 2.0 \n")
        f.write("FEM simulated result \n")
        f.write("DATASET UNSTRUCTURED_GRID \n")

        ### Nodes ###
        f.write("POINTS {} float \n".format(fem.nnode))
        for node in fem.nodes:
            f.write("{} {} {} \n".format(node.xyz[0],node.xyz[1],node.xyz[2]))

        ### Elements ###
        nsolid = 0
        nnode_max = 0
        for element in fem.elements:
            if "solid" in element.style:
                element.calc_stress()
                nsolid += 1
                nnode_max = max(nnode_max,element.nnode)

        f.write("CELLS {} {} \n".format(nsolid,nnode_max+1))
        for element in fem.elements:
            if "solid" in element.style:
                f.write("{} ".format(element.nnode))
                for node in element.nodes:
                    f.write("{} ".format(node.id))
                f.write("\n")

        f.write("CELL_TYPES {} \n".format(nsolid))
        for element in fem.elements:
            if "3d8solid" in element.style:
                f.write("{} \n".format(12))

        ### Node Data ###
        f.write("POINT_DATA {} \n".format(fem.nnode))
        f.write("VECTORS displacement float \n")
        f.write("LOOKUP_TABLE default \n")
        for node in fem.nodes:
            f.write("{} {} {} \n".format(node.u[0],node.u[1],node.u[2]))

        ### Element Data ###
        f.write("CELL_DATA {} \n".format(nsolid))
        f.write("TENSORS strain float \n")
        f.write("LOOKUP_TABLE default \n")
        for element in fem.elements:
            if "solid" in element.style:
                f.write("{} {} {} \n".format(element.strain[0],element.strain[3],element.strain[5]))
                f.write("{} {} {} \n".format(element.strain[3],element.strain[1],element.strain[4]))
                f.write("{} {} {} \n".format(element.strain[5],element.strain[4],element.strain[2]))

        f.write("CELL_DATA {} \n".format(nsolid))
        f.write("TENSORS stress float \n")
        f.write("LOOKUP_TABLE default \n")
        for element in fem.elements:
            if "solid" in element.style:
                f.write("{} {} {} \n".format(element.stress[0],element.stress[3],element.stress[5]))
                f.write("{} {} {} \n".format(element.stress[3],element.stress[1],element.stress[4]))
                f.write("{} {} {} \n".format(element.stress[5],element.stress[4],element.stress[2]))