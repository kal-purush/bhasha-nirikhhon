import sources
import targets
import transformations

def processMapFile(map_file):
    map_context = map_file.xpathNewContext()

    source_processor = sources.SourceProcessor(map_context)
    source = source_processor.getConfiguredProcessor()

    target_processor = targets.TargetProcessor(map_context)
    target = target_processor.getConfiguredProcessor()

    transformation_processor = transformations.TransformationProcessor(map_context)
    transformation = transformation_processor.getConfiguredProcessor()

    # Procesar los campos
    map_fields = map_context.xpathEval("/map/fields/field")
    field_ids = []
    for f in map_fields:
        field_id = map_fields.index(f)
        field_ids.append(field_id)
        
        source.confField(field_id, f)
        transformation.confField(field_id, f)
        target.confField(field_id, f)
    
    
    while(source.nextRow() and target.nextRow() and transformation.nextRow()):
        all_valid = True
        for field_id in field_ids:
            data = source.processField(field_id, None)
            
            all_valid = all_valid and data[1]
            
            data = transformation.processField(field_id, data[0])
            target.processField(field_id, data[0])
            
        #Si no hay datos validos para este campo, se deshecha la fila completa
        if(not all_valid):
            print "Deshechada la linea [" + source.line + "]"
            source.dropRow()
            target.dropRow()
            transformation.dropRow()
        
#        print ".",
    
    target.flush()

