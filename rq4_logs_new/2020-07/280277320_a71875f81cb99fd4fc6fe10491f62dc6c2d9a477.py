#!C:\Program Files (x86)\Python
# if this does not recognize Mac setting fill-in the route for Python
# written by Daemon Dikeman, and revised +tested by Sushma Naithani on May 2020

#Dependencies
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import sys

#Parameter setting: # establishing some base level args. Prepare .CSV file for uploading.
if __name__ == '__main__':

    #XL_Name = r"C:\Users\Naithans\Desktop\filename.csv: this how it will work in windows
    
    XL_Name = r"/Users/Naithans/Desktop/Filename.csv" 
    sys.argv = ['MultiCluster.py']
    Basep1Flag = True #this means the values in the pluged-in file are not Log2 transformed
    Log2p1Flag = True # Log transformation is needed
    Log2p2Flag = True # true means cluster X sides and False means no clustering 
    Corrp1Flag = False # what if true
    Corrp2Flag = False # what if true
    metrix = 'correlation'
    
    #there are different metrix that can be used, e.g.  correlation, eucleadian etc.

#Preparing for clustering
    XL_Handle = open(XL_Name, "r")

    XL_BaseTable = pd.read_csv(filepath_or_buffer=XL_Handle, index_col=0)
    XL_BaseTable.replace(to_replace=0, value=0.1, inplace=True) # empty spaces will not be afftected by this treatment.
    XL_Log2Table = np.log2(XL_BaseTable) 
    XL_BaseTable.replace(to_replace=0.1, value=0, inplace=True)
    XL_BaseTableNANless = XL_BaseTable.fillna(value=0) # the empty space in the row will be automatically treated as NA, we do not need to add this.

    XL_Log2TableNANless = XL_Log2Table.fillna(value=-3) 

    # grid_kws = {"height_ratios": (.9, .05), "hspace": .3}: this means that fig will be 90% and the scale bar will be 5%
    # f, (ax, cbar_ax) = plt.subplots(2, gridspec_kw=grid_kws)
    
#Base1Flag is true means that if you want the base table as a source that is not log adjusted. If set false then this step is skipped and Log2 table will be used for graph 
    if Basep1Flag is True:
        BaseX = sns.clustermap(XL_BaseTableNANless.transpose(), # for better visualization rows are converted into columns and columns into rows
                               metric=metrix, #this is the distance between two variables
                               cmap="viridis", #this is color schema (name couple color schema/reference link)
                               figsize=[18, 8], #size of the fig/aspect ratio
                               row_cluster=False, # the rrows now corresponds to the columns in the original CSV file (tissue samples and not genes)
                        
                
                               xticklabels=True, #tick all the genes
                               cbar_kws={'label': 'TPM Values',
                                         
                                         }
                               )
        BaseDendroBox = BaseX.ax_row_dendrogram.get_position() #adjusting empty space from dendrogram that we didn't use
        BaseDendroBox.x0 = (BaseDendroBox.x0 + 9 * BaseDendroBox.x1) / 10
        BaseX.cax.set_position(BaseDendroBox)
        BaseX.cax.yaxis.set_ticks_position("left")
        BaseX.cax.yaxis.set_label_position("left")
        # BaseX.cax.set_aspect(10, anchor="W", adjustable="box")
        BaseX.ax_row_dendrogram.set_visible(False)
        # BaseX.ax_heatmap.set_title("SDRLK Baseline Gene Expression", fontsize=25, verticalalignment='top', pad=80)
        # BaseX.ax_heatmap.set_ylabel("Tissue Type (abbreviated)", fontsize=20)
        # BaseX.ax_heatmap.set_xlabel("Gene ID", fontsize=20)
        BaseX.ax_heatmap.tick_params(axis='x', labelsize=8) # range for label 8-12
        '''# split axes of heatmap to put colorbar
        ax_divider = make_axes_locatable(BaseX.ax_heatmap)
        # define size and padding of axes for colorbar
        cax = ax_divider.append_axes('top', size='5%', pad='2%')
        # make colorbar for heatmap.
        # Heatmap returns an axes obj but you need to get a mappable obj (get_children)
        colorbar(BaseX.ax_heatmap.get_children()[0], cax=cax, orientation='horizontal', ticks=[0, 30, 60, 90, 120, 150])
        # locate colorbar ticks
        cax.xaxis.set_ticks_position('top')
        cax.set_label("TPM Values")'''
        plt.savefig("Base+_Expression{metrix}.png", # Daemon: TODO ask program to use same name as csv-file
                    bbox_inches='tight'
                    )
#If you want to maintain the order of the rows (in transpose table tissue types) and not cluster those then Log2p1Flag will be set true, but the genes (column will be clustered)
    if Log2p1Flag is True:
        Log2X = sns.clustermap(XL_Log2TableNANless.transpose(),
                               metric=metrix,
                               cmap="viridis",
                               figsize=[18, 8],
                               row_cluster=False,
                               col_cluster=True,
                               xticklabels=True,
                               cbar_kws={"label": "Log2 TPM Values",
                                         }
                               )
        Log2X.ax_row_dendrogram.set_visible(False)
        Log2DendroBox = Log2X.ax_row_dendrogram.get_position()
        Log2DendroBox.x0 = (Log2DendroBox.x0 + 9 * Log2DendroBox.x1) / 10   # TODO if someone wants to flip the label bar on side replace this line with the code:
        # Log2DendroBox.x1 -= 0.03 : this was playyed with to place heading bar with respect to main fig.
        print(Log2DendroBox)
        Log2X.cax.set_position(Log2DendroBox)
        Log2X.cax.yaxis.set_ticks_position("left")
        Log2X.cax.yaxis.set_label_position("left")
        Log2X.cax.set_position(Log2DendroBox)
        # Log2X.ax_heatmap.set_title("SDRLK Baseline Gene Expression", fontsize=25, pad=60)
        # Log2X.ax_heatmap.set_ylabel("Tissue Type (abbreviated)", fontsize=20)
        # Log2X.ax_heatmap.set_xlabel("Gene ID", fontsize=20)
        Log2X.ax_heatmap.set_xlabel("")
        Log2X.ax_heatmap.tick_params(axis="x", labelsize=8) # gene ID font can be changed here from 8 to 12, but it will need adjustment in the line 89
        plt.savefig("Log2_Expression+_filename_{metrix}.png", # TODO: filename should be extracted from csv file.
                    bbox_inches='tight'
                    )
                    
                    #if Log2p2Flag is True: if you want to do clustering of the tissues (row) then this script will follow

    if Log2p2Flag is True:
        Log2X = sns.clustermap(XL_Log2TableNANless.transpose(),
                               metric=metrix,
                               cmap="viridis",
                               figsize=[18, 8],
                               xticklabels=True,
                               cbar_kws={"label": "Log2 TPM Values"}
                               )
        Log2DendroBox = Log2X.ax_row_dendrogram.get_position()
        Log2DendroBox.x0 = (Log2DendroBox.x0 + 9 * Log2DendroBox.x1) / 10
        Log2DendroWid = Log2DendroBox.x1 - Log2DendroBox.x0
        Log2DendroBox.x1 = Log2X.ax_row_dendrogram.get_position().x0
        Log2DendroBox.x0 = Log2DendroBox.x1 - Log2DendroWid
        print(Log2DendroBox)
        Log2X.cax.set_position(Log2DendroBox)
        Log2X.cax.yaxis.set_ticks_position("left")
        Log2X.cax.yaxis.set_label_position("left")
        Log2X.cax.set_position(Log2DendroBox)
        # Log2X.ax_heatmap.set_title("Baseline Gene Expression", fontsize=25, pad=60) (this can be made active code by removing the # if you need grraph to be labeled)
        # Log2X.ax_heatmap.set_ylabel("Tissue Type (abbreviated)", fontsize=20)
        # Log2X.ax_heatmap.set_xlabel("Gene ID", fontsize=20)
        Log2X.ax_heatmap.set_xlabel("") # this is important to keep the program away from generating auto labels.
        Log2X.ax_heatmap.tick_params(axis="x", labelsize=8)
        plt.savefig("Log2X_expression_gene+_ClusTiss_{metrix}.png",
                    bbox_inches='tight'
                    )
#The follwoing script is taking into account of tissues for correlation for clustering 
#TODO: explain a bit , what happens if any is set to False.  Or if leaving these true alwayys is useful.

    if Corrp1Flag is True: 
        df = XL_Log2TableNANless.corr()
        df.to_csv(path_or_buf="CorrX_GenesXX_Tiss_Network.csv", mode="w")
        CorrX = sns.heatmap(XL_Log2TableNANless.corr(),
                            vmin=-1,
                            cmap='coolwarm',
                            # annot=True,
                            )
        plt.savefig("CorrX_GenesXXX+_Tiss.png",
                    bbox_inches='tight'
                    )
    if Corrp2Flag is True:
        CorrXTable = XL_Log2TableNANless.transpose()
        plt.show()
        CorrX = sns.heatmap(CorrXTable.corr(),
                            vmin=-1,
                            cmap='coolwarm',
                            # annot=True,
                            )
        plt.savefig("CorrX_GenesXX+_Express.png",
                    bbox_inches='tight'
                    )