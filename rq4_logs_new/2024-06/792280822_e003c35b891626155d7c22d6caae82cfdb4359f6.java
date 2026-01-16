package com.akaene.stpa.scs.parser.sysml;

import com.akaene.stpa.scs.exception.ControlStructureParserException;
import com.akaene.stpa.scs.model.AggregationType;
import com.akaene.stpa.scs.model.Association;
import com.akaene.stpa.scs.model.AssociationEnd;
import com.akaene.stpa.scs.model.Component;
import com.akaene.stpa.scs.model.ComponentType;
import com.akaene.stpa.scs.model.ConnectorEnd;
import com.akaene.stpa.scs.model.Model;
import com.akaene.stpa.scs.model.Stereotype;
import com.akaene.stpa.scs.parser.ControlStructureParser;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.impl.DynamicEObjectImpl;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.uml2.uml.AggregationKind;
import org.eclipse.uml2.uml.Class;
import org.eclipse.uml2.uml.ConnectableElement;
import org.eclipse.uml2.uml.Connector;
import org.eclipse.uml2.uml.NamedElement;
import org.eclipse.uml2.uml.Port;
import org.eclipse.uml2.uml.PrimitiveType;
import org.eclipse.uml2.uml.Property;
import org.eclipse.uml2.uml.resource.XMI2UMLResource;
import org.eclipse.uml2.uml.resources.util.UMLResourcesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * SysML XMI parser using the Eclipse Modeling Framework (EMF).
 */
public class EMFSysMLXMIParser implements ControlStructureParser {

    private static final Logger LOG = LoggerFactory.getLogger(EMFSysMLXMIParser.class);

    static {
        UMLResourcesUtil.initGlobalRegistries();
    }

    @Override
    public Model parse(File input) {
        final Resource xmi = parseAsResource(input);
        final org.eclipse.uml2.uml.Model emfModel = getModelElement(xmi);
        final ParsingState state = new ParsingState();
        extractModelMetadata(emfModel, state);
        extractStereotypes(xmi, state);
        extractClasses(emfModel, state);
        extractConnectors(emfModel, state);
        extractAssociations(emfModel, state);
        LOG.debug("Parsed model:\n{}", state.result);
        return state.result;
    }

    public Resource parseAsResource(File input) {
        LOG.debug("Parsing XMI file '{}'.", input);
        ResourceSet set = new ResourceSetImpl();
        Stream.of(SysMLXMIParser.SUPPORTED_EXTENSIONS)
              .forEach(ext -> set.getResourceFactoryRegistry().getExtensionToFactoryMap()
                                 .put(ext, XMI2UMLResource.Factory.INSTANCE));
        try {
            final URI uri = URI.createFileURI(input.getAbsolutePath());
            return set.getResource(uri, true);
        } catch (RuntimeException e) {
            throw new ControlStructureParserException("Unable to parse file " + input, e);
        }
    }

    private org.eclipse.uml2.uml.Model getModelElement(Resource xmi) {
        return xmi.getContents().stream().filter(org.eclipse.uml2.uml.Model.class::isInstance).map(
                org.eclipse.uml2.uml.Model.class::cast).findFirst().orElseThrow(
                () -> new ControlStructureParserException(
                        "Input does not have the expected structure. Expected top level model element."));
    }

    private void extractModelMetadata(org.eclipse.uml2.uml.Model xmiModel, ParsingState state) {
        state.result.setName(xmiModel.getName());
    }

    private void extractStereotypes(Resource xmi, ParsingState state) {
        xmi.getContents().stream()
           .filter(o -> o instanceof DynamicEObjectImpl)
           .map(DynamicEObjectImpl.class::cast)
           .forEach(stereotype -> {
               final Stereotype result = new Stereotype(stereotype.eClass().getName());
               state.result.addStereotype(result);
               state.stereotypes.put(stereotype, result);
           });
    }

    private void extractClasses(org.eclipse.uml2.uml.Model xmiModel, ParsingState state) {
        xmiModel.allOwnedElements().stream()
                .filter(o -> o instanceof Class)
                .map(o -> (Class) o)
                .map(cls -> {
                    final ComponentType ct = new ComponentType(cls.getName(), cls.getQualifiedName());
                    getElementStereotypes(cls, state).forEach(ct::addStereotype);
                    return ct;
                })
                .forEach(state.result::addClass);
        resolveSupertypesAndParts(xmiModel, state);
    }

    private void resolveSupertypesAndParts(org.eclipse.uml2.uml.Model xmiModel, ParsingState state) {
        xmiModel.allOwnedElements().stream()
                .filter(o -> o instanceof Class)
                .map(Class.class::cast)
                .forEach(cls -> {
                    final Optional<ComponentType> target = state.result.getClass(cls.getName());
                    assert target.isPresent();
                    getSuperTypes(cls, state).forEach(ct -> target.get().addSuperType(ct));
                    final Collection<Association> partOf = extractAttributeAssociations(cls, state);
                    partOf.forEach(association -> target.get().addAttribute(association));
                });
    }

    private Collection<Stereotype> getElementStereotypes(Object element, ParsingState state) {
        return state.stereotypes.entrySet().stream().filter(e -> hasStereotype(e.getKey(), element))
                                .map(Map.Entry::getValue).toList();
    }

    private static boolean hasStereotype(DynamicEObjectImpl stereotypeElem, Object element) {
        int max = stereotypeElem.eClass().getEAllStructuralFeatures().size();

        for (int i = 0; i < max; i++) {
            Object p = stereotypeElem.dynamicGet(i);
            if (p == element) {
                return true;
            }
        }
        return false;
    }

    private Collection<ComponentType> getSuperTypes(Class cls, ParsingState state) {
        return cls.getSuperClasses().stream().map(supertype -> state.result.getClass(supertype.getName())).flatMap(
                Optional::stream).toList();
    }

    private Collection<Association> extractAttributeAssociations(Class cls, ParsingState state) {
        return cls.getAllAttributes().stream().filter(p -> !(p instanceof Port)).map(part -> {
            final AssociationEnd target = propertyToAssociationEnd(part, state);
            final AssociationEnd source;
            if (part.getOtherEnd() != null) {
                source = propertyToAssociationEnd(part.getOtherEnd(), state);
            } else {
                final Optional<ComponentType> sourceType = state.result.getClass(cls.getName());
                assert sourceType.isPresent();
                source = new AssociationEnd(sourceType.get(), AggregationType.ASSOCIATION,
                                            null, 0, null);
            }
            final Optional<org.eclipse.uml2.uml.Association> assocElement = Optional.ofNullable(part.getAssociation());
            final Association association = new Association(
                    assocElement.map(NamedElement::getName).orElse(null), assocElement.map(
                    NamedElement::getQualifiedName).orElse(part.getQualifiedName()), source, target);
            getElementStereotypes(part.getAssociation(), state).forEach(association::addStereotype);
            return association;
        }).toList();
    }

    private ComponentType propertyType(Property property, ParsingState state) {
        return Optional.ofNullable(property.getType()).flatMap(ct -> state.result.getClass(ct.getName()))
                       .orElseGet(() -> {
                           if (property.getType() instanceof PrimitiveType) {
                               return new ComponentType(property.getType().getName(),
                                                        property.getType().getQualifiedName());
                           }
                           return ComponentType.UNSPECIFIED;
                       });
    }

    private AssociationEnd propertyToAssociationEnd(Property property, ParsingState state) {
        final ComponentType targetType = propertyType(property, state);
        return new AssociationEnd(targetType, aggregationType(property.getAggregation()),
                                  property.getName(), property.getLower(), property.getUpper());
    }

    private static AggregationType aggregationType(AggregationKind emfAggregation) {
        return switch (emfAggregation) {
            case NONE_LITERAL -> AggregationType.ASSOCIATION;
            case SHARED_LITERAL -> AggregationType.AGGREGATION;
            case COMPOSITE_LITERAL -> AggregationType.COMPOSITION;
        };
    }

    private void extractConnectors(org.eclipse.uml2.uml.Model xmiModel, ParsingState state) {
        final List<Connector> connectors = xmiModel.allOwnedElements().stream()
                                                   .filter(o -> o instanceof org.eclipse.uml2.uml.Connector)
                                                   .map(org.eclipse.uml2.uml.Connector.class::cast)
                                                   .toList();
        connectors.stream().map(c -> {
            assert c.getEnds().size() == 2;

            final Optional<ConnectorEnd> source = connectorEnd(c.getEnds().getFirst(), state);
            final Optional<ConnectorEnd> target = connectorEnd(c.getEnds().get(1), state);
            if (source.isEmpty() || target.isEmpty()) {
                return null;
            }
            final com.akaene.stpa.scs.model.Connector connector = new com.akaene.stpa.scs.model.Connector(c.getName(),
                                                                                                          c.getQualifiedName(),
                                                                                                          source.get(),
                                                                                                          target.get());
            getElementStereotypes(c.getEnds().getFirst(), state).forEach(connector::addStereotype);
            getElementStereotypes(c.getEnds().get(1), state).forEach(connector::addStereotype);
            return connector;
        }).filter(Objects::nonNull).forEach(state.result::addConnector);
    }

    private Optional<ConnectorEnd> connectorEnd(org.eclipse.uml2.uml.ConnectorEnd umlConnectorEnd, ParsingState state) {
        final ConnectableElement connected = umlConnectorEnd.getRole() != null ? umlConnectorEnd.getRole() :
                                             umlConnectorEnd.getPartWithPort();
        if (connected == null) {
            return Optional.empty();
        }
        final Optional<ComponentType> type =
                connected.getType() != null ? state.result.getClass(connected.getType().getName()) : Optional.empty();

        final Component comp = state.components.computeIfAbsent(
                connected,
                k -> new Component(connected.getName(), connected.getQualifiedName(),
                                   type.orElse(ComponentType.UNSPECIFIED))
        );
        return Optional.of(new ConnectorEnd(comp, null, umlConnectorEnd.getLower(), umlConnectorEnd.getUpper()));
    }

    private void extractAssociations(org.eclipse.uml2.uml.Model xmiModel, ParsingState state) {
        final List<org.eclipse.uml2.uml.Association> associations = xmiModel.allOwnedElements().stream()
                                                                            .filter(o -> o instanceof org.eclipse.uml2.uml.Association)
                                                                            .map(org.eclipse.uml2.uml.Association.class::cast)
                                                                            .toList();
        final List<Association> result = associations.stream().map(a -> {
            assert a.getMemberEnds().size() == 2;
            final AssociationEnd source = propertyToAssociationEnd(a.getMemberEnds().getFirst(), state);
            final AssociationEnd target = propertyToAssociationEnd(a.getMemberEnds().get(1), state);
            final Association association = new Association(a.getName(),
                                                            a.getQualifiedName() != null ? a.getQualifiedName() :
                                                            a.getMemberEnds().getFirst().getQualifiedName(), source,
                                                            target);
            getElementStereotypes(a, state).forEach(association::addStereotype);
            return association;
        }).filter(association -> !state.result.getAssociations().contains(association)).toList();
        result.forEach(state.result::addAssociation);
    }

    protected static final class ParsingState {

        private final Model result = new Model();

        private final Map<Object, Component> components = new HashMap<>();

        private final Map<DynamicEObjectImpl, Stereotype> stereotypes = new HashMap<>();
    }
}