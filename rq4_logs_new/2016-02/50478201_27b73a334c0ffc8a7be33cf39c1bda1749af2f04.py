import arcpy

myworkspace=None #r'C:\data\projects\DeLand\2-9-16.gdb'
inputFeatures='Street'
targetDescriptionField='HTMLDESC'
pivottable='PIVOT'
pivotGISKeyField='ID'
filenameField='PLAN'
fileExtension='.pdf'
scanstable='scans'
scansurl='http://gis2.vhb.com/projects/deland/scans/'


try:
    
    
    inputFeatures = arcpy.GetParameterAsText(0)
    arcpy.AddMessage("Features: " + inputFeatures)

    targetDescriptionField = arcpy.GetParameterAsText(1)
    arcpy.AddMessage("Target Description Field: " + targetDescriptionField)

    pivottable = arcpy.GetParameterAsText(2)
    arcpy.AddMessage("PIVOT Table: " + pivottable)

    scanstable = arcpy.GetParameterAsText(3)
    arcpy.AddMessage("Scans Table: " + scanstable)

    filenameField = arcpy.GetParameterAsText(4)
    arcpy.AddMessage("File Name Field: " + filenameField)

    pivotGISKeyField = arcpy.GetParameterAsText(5)
    arcpy.AddMessage("GIS Key Field: " + pivotGISKeyField)

    fileExtension = arcpy.GetParameterAsText(6)
    arcpy.AddMessage("File Extension: " + fileExtension)

    scansurl = arcpy.GetParameterAsText(7)
    arcpy.AddMessage("Scans Web Server: " + scansurl)

    mxd = arcpy.mapping.MapDocument("CURRENT")
    df = arcpy.mapping.ListDataFrames(mxd)[0]

    for lyr in arcpy.mapping.ListLayers(mxd, "", df):
        if lyr.name == inputFeatures:
            if myworkspace==None: myworkspace = lyr.workspacePath

    arcpy.env.workspace = myworkspace #set workspace

    edit=arcpy.da.Editor(myworkspace) 
    edit.startEditing()
    
#for each street
    updateFields=['OBJECTID',targetDescriptionField]
    with arcpy.da.UpdateCursor(inputFeatures,updateFields ) as selectedInputFeatures:
        for row in selectedInputFeatures:

            oid=row[0]
            arcpy.AddMessage("processing feature " + str(oid) )

    #init list of scans
            sDesc=''
            iDocs=0
    #get ATT_NAMEs from PIVOT

            #where='ID = ' + str(oid)
            where=pivotGISKeyField + ' = ' + str(oid)
    #get attachment feature using query and calculate URL
            arcpy.AddMessage('Search for PIVOTs: ' + where)
            with arcpy.da.SearchCursor(pivottable,[pivotGISKeyField,filenameField],where) as pivotCursor:
                
                for rowPivot in pivotCursor:

                    v=rowPivot[1];
                    v=str(rowPivot[1]).replace("'","''")
                    #where2="PLAN='" + v + "'"
                    where2= filenameField + "='" + v + "'"
                    arcpy.AddMessage('Search for Scans: ' + where2)
                    with arcpy.da.SearchCursor(scanstable,['OBJECTID',filenameField],where2) as attachCursor:
                        
                        for rowAttach in attachCursor:
                            aid=rowAttach[0]
                            #arcpy.AddMessage(aid)
                            attname=rowAttach[1]
                            #arcpy.AddMessage(attname)
                            attachURL=scansurl + attname + fileExtension# '/' + str(rowAttach[1]) + '/attachments/' + str(aid)
                            #arcpy.AddMessage(attachURL)
                            sDesc=sDesc +  "<a href='" + attachURL + "' />" + attname  + "</a></br>"
                            
                            iDocs=iDocs+1
                            #arcpy.AddMessage(iDocs)

    #calculate local street description field
                sDesc=str(iDocs) + " Related Document(s):</br>" + sDesc

                if iDocs>0:
                    row[1]=sDesc
                    arcpy.AddMessage(sDesc)
                    edit.startOperation()
                    selectedInputFeatures.updateRow(row)
                    edit.stopOperation()
    
    edit.stopEditing(True)
    if selectedInputFeatures: del selectedInputFeatures
    if attachCursor: del attachCursor
    if pivotCursor:del pivotCursor

    arcpy.AddMessage ("Complete.")

except arcpy.ExecuteError:
    arcpy.AddMessage ("Finished with Error.")
    print(arcpy.GetMessages(2))
    edit.stopEditing(True)
    