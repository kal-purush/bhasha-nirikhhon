package de.samply.fhir2mdr.xml;

import de.samply.fhir2mdr.model.*;
import de.samply.fhir2mdr.model.DataElement;
import de.samply.fhir2mdr.model.Element;
import de.samply.fhir2mdr.model.Namespace;
import de.samply.fhir2mdr.model.PermissibleValue;
import de.samply.schema.mdr.common.*;

import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;

public class ModelToXSDObjects {

    private String namespaceUUID;

    private ObjectFactory objectFactory = new ObjectFactory();

    private int runningId = 0;

    public Export convert(Namespace namespace){

        Export export = new Export();
        convert(namespace,export);
        return export;
    }

    private void convert(Namespace namespace, Export export){

        ObjectFactory objectFactory = new ObjectFactory();
        de.samply.schema.mdr.common.Namespace xmlNamespace = new de.samply.schema.mdr.common.Namespace();

        setNewUUID(xmlNamespace);
        this.namespaceUUID = xmlNamespace.getUuid();

        xmlNamespace.setName(namespace.getName());

        xmlNamespace.setDefinitions(convertDefinitions(namespace,this.namespaceUUID));

        export.getElement().add(objectFactory.createNamespace(xmlNamespace));

        for(ElementWithSlots elem: namespace.getMembers()){
            if(elem.getClass().equals(Group.class)){
                Group group = (Group) elem;
                convert(group,export);
            }
            if(elem.getClass().equals(DataElement.class)){
                DataElement de = (DataElement) elem;
                convert(de,export);
            }
        }
    }

    private String convert(Group group, Export export){

        //Each group consists of the group itself....
        DataElementGroup xmlGroup = new DataElementGroup();
        setNewUUID(xmlGroup);

        export.getElement().add(objectFactory.createDataElementGroup(xmlGroup));

        //And an scopedIdentifier linked by UUIDs
       ScopedIdentifier groupIdentifier = extractScopedIdentifer(group,xmlGroup.getUuid());

        export.getElement().add(objectFactory.createScopedIdentifier(groupIdentifier));

        for(ElementWithSlots elem: group.getMembers()){
            if(elem.getClass().equals(Group.class)){
                Group subGroup = (Group) elem;
                groupIdentifier.getSub().add(convert(subGroup,export));
            }
            if(elem.getClass().equals(DataElement.class)){
                DataElement de = (DataElement) elem;
                groupIdentifier.getSub().add(convert(de,export));
            }
        }

        return groupIdentifier.getUuid();

    }

    private String convert(DataElement dataElement,Export export){
        de.samply.schema.mdr.common.DataElement xmlElement = new de.samply.schema.mdr.common.DataElement();
        setNewUUID(xmlElement);
        ScopedIdentifier identifier = extractScopedIdentifer(dataElement, xmlElement.getUuid());
        export.getElement().add(objectFactory.createScopedIdentifier(identifier));
        xmlElement.setValueDomain(extractValueDomain(dataElement, identifier, export));
        export.getElement().add(objectFactory.createDataElement(xmlElement));

        return identifier.getUuid();


    }

    private String extractValueDomain(DataElement de, ScopedIdentifier deIdentifier, Export export){
        IValidationType val = de.getValidation();
        if (val.getType().equals(new EnumeratedType().getType())){
            return extractEnumeratedDomain((EnumeratedType) val, deIdentifier, export);
        }

        DescribedValueDomain xmlVal = new DescribedValueDomain();
        setNewUUID(xmlVal);

        if(val.getType().equals(new BooleanType().getType())){
            //TODO CHECK
            xmlVal.setDatatype("BOOLEAN");
            xmlVal.setFormat("");
            xmlVal.setMaxCharacters(BigInteger.ZERO);
            xmlVal.setDescription("TRUE/FALSE");
            xmlVal.setValidationType("NONE");
            xmlVal.setValidationData("");
        }
        if(val.getType().equals(new CalendarType().getType())){
            CalendarType calVal = (CalendarType) val;
            fillAttributes(calVal.getFormat(),xmlVal);

        }
        if(val.getType().equals(new FloatType().getType())){
            FloatType fVal = (FloatType) val;
            if(fVal.getRangeFrom() != null && fVal.getRangeTo() != null){
                String range = fVal.getRangeFrom().toString()+"<=x<="+fVal.getRangeTo().toString();
                xmlVal.setFormat(range);
                xmlVal.setDescription(range);
                xmlVal.setValidationData(range);
                xmlVal.setValidationType("FLOATRANGE");
            }else{
                xmlVal.setFormat("x");
                xmlVal.setDescription("x");
                xmlVal.setValidationType("FLOAT");
                xmlVal.setValidationData("x");
            }

            xmlVal.setDatatype("FLOAT");
            xmlVal.setMaxCharacters(BigInteger.valueOf(10));
            if(fVal.getUnitOfMeasure() == null){
                xmlVal.setUnitOfMeasure("");
            }else {
                xmlVal.setUnitOfMeasure(fVal.getUnitOfMeasure());
            }
        }
        if(val.getType().equals(new IntegerType().getType())){
            IntegerType iVal = (IntegerType) val;
            if(iVal.getRangeFrom() != null && iVal.getRangeTo() != null){
                String range = iVal.getRangeFrom().toString() +"<=x<="+iVal.getRangeTo().toString();
                xmlVal.setFormat(range);
                xmlVal.setDescription(range);
                xmlVal.setValidationType("INTEGERRANGE");
                xmlVal.setValidationData(range);
            }else{
                xmlVal.setFormat("");
                xmlVal.setDescription("No limits specified");
                xmlVal.setValidationType("NONE");
            }
            xmlVal.setDatatype("INTEGER");
            xmlVal.setMaxCharacters(BigInteger.ZERO);
            if(iVal.getUnitOfMeasure() == null){
                xmlVal.setUnitOfMeasure("");
            }else {
                xmlVal.setUnitOfMeasure(iVal.getUnitOfMeasure());
            }

        }
        if(val.getType().equals(new StringType().getType())){
            StringType stringVal = (StringType) val;
            xmlVal.setDatatype("STRING");
            xmlVal.setFormat("");
            xmlVal.setMaxCharacters(BigInteger.valueOf(stringVal.getMaxLength()));
            xmlVal.setDescription("");
            if(stringVal.getRegex().isEmpty()){
                xmlVal.setValidationType("NONE");
                xmlVal.setValidationData("");
            }else{
                xmlVal.setValidationType("REGEX");
                xmlVal.setValidationData(stringVal.getRegex());
            }
        }

        export.getElement().add(objectFactory.createDescribedValueDomain(xmlVal));
        return xmlVal.getUuid();
    }

    private String extractEnumeratedDomain(EnumeratedType val, ScopedIdentifier deIdentifier, Export export){
        EnumeratedValueDomain xmlVal = new EnumeratedValueDomain();
        setNewUUID(xmlVal);
        xmlVal.setFormat("enumerated");
        xmlVal.setDatatype("enumerated");
        xmlVal.setMaxCharacters(BigInteger.ZERO);

        for(PermissibleValue value:val.getValues()){
            de.samply.schema.mdr.common.PermissibleValue xmlValue = new de.samply.schema.mdr.common.PermissibleValue();
            setNewUUID(xmlValue);
            xmlValue.setValueDomain(xmlVal.getUuid());
            xmlValue.setValue(value.getPermittedValue());
            export.getElement().add(objectFactory.createPermissibleValue(xmlValue));

            //Labels for permissibleValues need to go into the DataElement scopedIdentifer
            Definitions valueDefinitions = convertDefinitions(value, xmlValue.getUuid());
            deIdentifier.getDefinitions().getDefinition().addAll(valueDefinitions.getDefinition());
        }

        export.getElement().add(objectFactory.createEnumeratedValueDomain(xmlVal));
        return xmlVal.getUuid();

    }



    private ScopedIdentifier extractScopedIdentifer(ElementWithSlots elem, String uuid ){
        ScopedIdentifier identifier = new ScopedIdentifier();
        setNewUUID(identifier);
        identifier.setStatus(Status.DRAFT);
        identifier.setDefinitions(convertDefinitions(elem, uuid));
        identifier.setSlots(convertSlots(elem));
        identifier.setNamespace(this.namespaceUUID);
        identifier.setIdentifier(getNextIdentifier());
        identifier.setVersion("1");
        identifier.setElement(uuid);
        return identifier;
    }

    private void setNewUUID(de.samply.schema.mdr.common.Element element){
        element.setUuid(UUID.randomUUID().toString());
    }

    private Definitions convertDefinitions(Element element, String uuid){
        Definitions definitions = new Definitions();
        for(Map.Entry<String,Label> e:element.getLabels().entrySet()){
            Definition def = new Definition();
            def.setLang(e.getKey());
            def.setDesignation(e.getValue().getDesignation());
            def.setDefinition(e.getValue().getDefinition());
            def.setUuid(uuid);
            definitions.getDefinition().add(def);
        }
        if(definitions.getDefinition().isEmpty()){
            return null;
        }
        return definitions;
    }

    private Slots convertSlots(ElementWithSlots element){
        Slots slots = new Slots();
        for(Map.Entry<String,String> e:element.getSlots().entrySet()){
            Slot slot = new Slot();
            slot.setKey(e.getKey());
            slot.setValue(e.getValue());
            slots.getSlot().add(slot);
        }
        if(slots.getSlot().isEmpty()){
            return null;
        }
        return slots;
    }

    private String getNextIdentifier(){
        this.runningId = runningId+1;
        return Integer.toString(runningId);
    }

    public DescribedValueDomain fillAttributes(DateTimeFormatEnum format, DescribedValueDomain toBeFilled) {

        //TODO Test
        switch (format) {
            case MONTH_YEAR:
                toBeFilled.setFormat("YYYY-MM");
                toBeFilled.setDatatype("DATE");
                toBeFilled.setMaxCharacters(BigInteger.valueOf(7));
                toBeFilled.setDescription("YYYY-MM");
                toBeFilled.setValidationType("DATE");
                toBeFilled.setValidationData("ISO_8601");
                return toBeFilled;
            case DAY_MONTH_YEAR:
                toBeFilled.setFormat("YYYY-MM-DD");
                toBeFilled.setDatatype("DATE");
                toBeFilled.setMaxCharacters(BigInteger.valueOf(10));
                toBeFilled.setDescription("YYYY-MM-DD");
                toBeFilled.setValidationType("DATE");
                toBeFilled.setValidationData("ISO_8601");
                return toBeFilled;
            case DATE_HOURS_MINUTES:
                toBeFilled.setFormat("YYYY-MM-DD hh:mm");
                toBeFilled.setDatatype("DATETIME");
                toBeFilled.setMaxCharacters(BigInteger.valueOf(16));
                toBeFilled.setDescription("YYYY-MM-DD hh:mm");
                toBeFilled.setValidationType("DATETIME");
                toBeFilled.setValidationData("ISO_8601; HOURS_24");
                return toBeFilled;
            case DATE_HOURS_MINUTES_SECONDS:
                toBeFilled.setFormat("YYYY-MM-DD hh:mm:ss");
                toBeFilled.setDatatype("DATETIME");
                toBeFilled.setMaxCharacters(BigInteger.valueOf(19));
                toBeFilled.setDescription("YYYY-MM-DD hh:mm:ss");
                toBeFilled.setValidationType("DATETIME");
                toBeFilled.setValidationData("ISO_8601; HOURS_24_WITH_SECONDS");
                return toBeFilled;
            default:
                return toBeFilled;
        }
    }

}