from aas import model
from aas.adapter import aasx
import aas
from aas.model import AssetKind, Constraint, ModelingKind, Reference, Submodel, SubmodelElement, SubmodelElementCollectionOrdered, SubmodelElementCollectionUnordered, Property, Identifier,Operation, SubmodelElementCollection, MultiLanguageProperty, Asset, AssetAdministrationShell, AASReference, Asset, Identifier, LangStringSet, Namespace, AdministrativeInformation, Security, Submodel, ConceptDictionary, View, AbstractObjectProvider  # Importa las clases necesarias
import abc
from typing import Optional, Set, Iterable, TYPE_CHECKING, List, Type
import math
from aas.model import base, datatypes
from aas.model.base import Constraint, LangStringSet, ModelingKind, Namespace, Reference
from aas.model.submodel import OperationVariable, SubmodelElement
if TYPE_CHECKING:
    from . import aas


class UE5G(Asset):
    def __init__(self, kind: AssetKind, identification: Identifier, id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, asset_identification_model: AASReference[Submodel] | None = None, bill_of_material: AASReference[Submodel] | None = None):
            super().__init__(kind, identification, id_short, category, description, parent, administration, asset_identification_model, bill_of_material)



class AASUE5G(AssetAdministrationShell):
    def __init__(self, asset_real,nameplate,identification_submodel,documentation,service,technicalData,simCard,ue5GIdentification,networkAccessRestrictions,ueAttachAndConnectionStatus,qos_monitoring, qosRequired, location, ue5GDatasheet, asset: AASReference[Asset], identification: Identifier, id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, security: Security | None = None, submodel: Set[AASReference[Submodel]] | None = None, concept_dictionary: Iterable[ConceptDictionary] = ..., view: Iterable[View] = ..., derived_from: AASReference[AssetAdministrationShell] | None = None):
       self.asset_real=asset_real
       self.nameplate=nameplate
       self.identification_submodel=identification_submodel
       self.documentation=documentation
       self.service=service
       self.technicalData=technicalData
       self.simCard=simCard
       self.ue5GIdentification=ue5GIdentification
       self.networkAccessRestrictions=networkAccessRestrictions
       self.ueAttachAndConnectionStatus=ueAttachAndConnectionStatus
       self.qosMonitoring=qos_monitoring
       self.qosRequired=qosRequired
       self.location=location
       self.ue5GDatasheet=ue5GDatasheet
       super().__init__(asset, identification, id_short, category, description, parent, administration, security, submodel, concept_dictionary, view, derived_from)
    
#UeAttachAndConnectionStatus Submodel

class QosParameters(SubmodelElementCollectionUnordered):


    def __init__(self, qos_identifier, arp, rqa, notification_control, gfbr_ul, gfbr_dl, mfbr_ul, mfbr_dl, max_packet_loss_rate, aggregate_bit_rates, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        
        self.qosIdentifier = qos_identifier
        self.arp = arp
        self.rqa = rqa
        self.notificationControl = notification_control
        self.gfbrul = gfbr_ul
        self.gfbrdl = gfbr_dl
        self.mfbrul = mfbr_ul
        self.mfbrdl = mfbr_dl
        self.maximumPacketLossRate = max_packet_loss_rate
        self.aggregateBitRates = aggregate_bit_rates
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
      

class QosCharacteristics(SubmodelElementCollectionUnordered):
    def __init__(self, resource_type, priority_level, packet_delay_budget, packet_error_rate, averaging_window, max_data_burst_volume, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.resourceType = resource_type
        self.priorityLevel = priority_level
        self.packetDelayBudget = packet_delay_budget
        self.packetErrorRate = packet_error_rate
        self.averagingWindow = averaging_window
        self.maximumDataBurstVolume = max_data_burst_volume
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
              

class QosProfile(SubmodelElementCollectionUnordered):
    def __init__(self, qos_parameters, qos_characteristics, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qosParameters = qos_parameters
        self.qosCharacteristics = qos_characteristics
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class AlternativeQosProfile(SubmodelElementCollectionUnordered):
    def __init__(self, qos_parameters, qos_characteristics, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qosParameters = qos_parameters
        self.qosCharacteristics = qos_characteristics
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class AlternativeQosProfiles(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.alternativeQosProfiles = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_alternative_qos_profile(self, alternative_qos_profile):
        self.alternativeQosProfiles.append(alternative_qos_profile)
    
    def remove_alternative_qos_profile(self, alternative_qos_profile):
        if alternative_qos_profile in self.alternativeQosProfiles:
            self.alternativeQosProfiles.remove(alternative_qos_profile)
            
        else:
            print("AlternativeQosProfile did not find in the list")


class Uplink(SubmodelElementCollectionUnordered):  #Lo tenía junto pero he tenido que cambiarlo porque sino para encontrarlo no había ninguna manera de diferenciar de uplink y downlink
    def __init__(self, mcstable, cqitable, target_bler, scheduling_type, scheduling_policy, harq_max_num_retransmissions, k_number, power_control_pmax, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.mcsTable = mcstable
        self.cqiTable = cqitable
        self.targetBLER = target_bler
        self.schedulingType = scheduling_type
        self.schedulingPolicy = scheduling_policy
        self.harqMaximumNumberOfRetransmissions = harq_max_num_retransmissions
        self.kNumber_ul = k_number
        self.powerControlPmax = power_control_pmax
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class Downlink(SubmodelElementCollectionUnordered):
    def __init__(self, mcstable, cqitable, target_bler, scheduling_type, scheduling_policy, harq_max_num_retransmissions, k_number, power_control_pmax, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.mcsTable = mcstable
        self.cqiTable = cqitable
        self.targetBLER = target_bler
        self.schedulingType = scheduling_type
        self.schedulingPolicy = scheduling_policy
        self.harqMaximumNumberOfRetransmissions = harq_max_num_retransmissions
        self.kNumber_ul = k_number
        self.powerControlPmax = power_control_pmax
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class RRMParameters(SubmodelElementCollectionUnordered):
    def __init__(self, uplink, downlink, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.uplink = uplink
        self.downlink = downlink
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             


class QosFlowStatus(SubmodelElementCollectionUnordered):                      
    def __init__(self, qfi, qos_profile, alternative_qos_profiles, rrm_parameters, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qfi = qfi              
        self.qosProfile = qos_profile
        self.alternativeQosProfiles = alternative_qos_profiles
        self.rrmParameters = rrm_parameters
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class QosFlowStatusList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qosFlows = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_qos_flow(self, qos_flow):
        self.qosFlows.append(qos_flow)

    def remove_qos_flow(self, qos_flow):
        if qos_flow in self.qosFlows:
            self.qosFlows.remove(qos_flow)
        else:
            print("QosFlow did not find in the list")


class PDUSessionStatus(SubmodelElementCollectionUnordered):
    def __init__(self,IPAddress,dnn, qos_flow_list, linkDirection, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.ipAddress=IPAddress 
        self.dnn=dnn
        self.linkDirection=linkDirection
        self.qosFlowList = qos_flow_list
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class PDUSessionStatusList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.pduSessions = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_pdu_session(self, pdu_session):
        self.pduSessions.append(pdu_session)


    def remove_pdu_session(self, pdu_session):
        if pdu_session in self.pduSessions:
            self.pduSessions.remove(pdu_session)

        else:
            print("PDU session did not find in the list")


class UeAttachAndConnectionStatus(Submodel):
    def __init__(self,UeAttached, RRC_state,pdu_session_list, technology, cellID, identification: base.Identifier,
                 submodel_element: Iterable[SubmodelElement] = (),
                 id_short: str = "",
                 category: Optional[str] = None,
                 description: Optional[base.LangStringSet] = None,
                 parent: Optional[base.Namespace] = None,
                 administration: Optional[base.AdministrativeInformation] = None,
                 semantic_id: Optional[base.Reference] = None,
                 qualifier: Optional[Set[base.Constraint]] = None,
                 kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.pduSessionList = pdu_session_list
        self.ueAttached= UeAttached
        self.rrcState= RRC_state
        self.technology=technology
        self.cellID=cellID
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


#NetworkAccessRestrictions Submodel

class ListOfCellGlobalIdentifier(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.cgis = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_cgi(self, cgi):
        self.cgis.append(cgi)

    def remove_cgi(self, cgi):
        if cgi in self.cgis:
            self.cgis.remove(cgi)
            
        else:
            print("CGI did not find in the list")



class ListOfPermissibleSlices(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.snssais = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_snssai(self, snssai):
        self.snssais.append(snssai)

    def remove_snssai(self, snssai):
        if snssai in self.snssais:
            self.snssais.remove(snssai)
            
        else:
            print("SNSSAI did not find in the list")

class NetworkAccessRestrictions(Submodel):
    def __init__(self,list_of_cgis, list_of_permissible_slices, identification: base.Identifier,
                 submodel_element: Iterable[SubmodelElement] = (),
                 id_short: str = "",
                 category: Optional[str] = None,
                 description: Optional[base.LangStringSet] = None,
                 parent: Optional[base.Namespace] = None,
                 administration: Optional[base.AdministrativeInformation] = None,
                 semantic_id: Optional[base.Reference] = None,
                 qualifier: Optional[Set[base.Constraint]] = None,
                 kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfCellGlobalIdentifier = list_of_cgis
        self.listOfPermissibleSlices = list_of_permissible_slices
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        
#QosMonitoring Submodel
class GeneralKeyPerformanceIndicators(SubmodelElementCollectionUnordered):
    def __init__(self, dropped_connections, traffic_volume_for_each_5qi_and_alternative_qos_profiles,handover_success_rate, sinr, data_throughput, bler, per, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.droppedConnections = dropped_connections
        self.trafficVolumeForEach5QIAndAlternativeQosProfiles = traffic_volume_for_each_5qi_and_alternative_qos_profiles
        self.handoverSuccessRate = handover_success_rate
        self.sinr = sinr
        self.dataThroughput = data_throughput
        self.bler = bler
        self.per = per
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class SignalLevel(SubmodelElementCollectionUnordered):
    def __init__(self, rssi, rsrp, rsrq, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.rssi = rssi
        self.rsrp = rsrp
        self.rsrq=rsrq
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class QosFlowMonitoring(SubmodelElementCollectionUnordered):                      
    def __init__(self, qfi, commservavai,commservrel, e2elm, e2elavg,survivaltime,servicebitrate,per,bler,updatetime,data_throughput, cause5gsm, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qfi = qfi              
        self.communicationServiceAvailabilty=commservavai
        self.communicationServiceReliability=commservrel
        self.endToEndLatencyMaximum=e2elm
        self.endToEndLatencyAVG=e2elavg
        self.survivalTime=survivaltime
        self.serviceBitRate=servicebitrate
        self.packetErrorRatio=per
        self.bler=bler
        self.updateTime=updatetime
        self.dataThroughput=data_throughput
        self.cause5GSM=cause5gsm
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class QosFlowMonitoringList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qosFlows = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_qos_flow(self, qos_flow):
        self.qosFlows.append(qos_flow)

    def remove_qos_flow(self, qos_flow):
        if qos_flow in self.qosFlows:
            self.qosFlows.remove(qos_flow)
            
        else:
            print("QosFlow did not find in the list")

class PDUSessionMonitoring(SubmodelElementCollectionUnordered):
    def __init__(self,IPAddress,qos_flow_list, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.ipAddress=IPAddress 
        self.qosFlowList = qos_flow_list
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class PDUSessionMonitoringList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.pduSessions = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_pdu_session(self, pdu_session):
        self.pduSessions.append(pdu_session)

    def remove_pdu_session(self, pdu_session):
        if pdu_session in self.pduSessions:
            self.pduSessions.remove(pdu_session)
            
        else:
            print("PDU session did not find in the list")


class ParametersPertainingConnections(SubmodelElementCollectionUnordered):
    def __init__(self, pdu_session_list, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.pduSessionList = pdu_session_list
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             


class QosQuery(Operation):
    def __init__(self, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
            
    #Functions in operations will developed later
    def query(self, qos_parameters, timespan):
        # Logic of the operation
        # result of the query
        result = None  #This will change
        return result

class ConnectivityQosMonitoringSubscription(Operation):
    def __init__(self, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
        

    def subscribe(self, qos_parameters, report_indicator, trigger_event):

        notification_message = None  
        return notification_message

class ConnectivityQosMonitoringResultsAndEvents(Operation):
    def __init__(self, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
        
    def process_event(self, event_notification_message):

        report = None  
        return report

class ListOfMonitoringSubscriptions(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.subscriptions = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_subscription(self, subscription):                                           
        self.subscriptions.append(subscription)

    def remove_subscription(self, subscription):
        if subscription in self.subscriptions:
            self.subscriptions.remove(subscription)

        else:
            print("Subscription did not find in the list")


class QosMonitoring(Submodel):
        def __init__(self, general_kpis, signal_level, parameters_pertaining_connections, qos_query, conecqosmonsubs,conecqosmonresandev,listOfMonitoringSubscriptions, updateTime, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
            self.generalKeyPerformanceIndicators = general_kpis
            self.signalLevel = signal_level
            self.parametersPertainingConnections = parameters_pertaining_connections
            self.qosQuery=qos_query
            self.connectivityQosMonitoringSubscription=conecqosmonsubs
            self.connectivityQosMonitoringResultsAndEvents=conecqosmonresandev
            self.listOfSubscriptions=listOfMonitoringSubscriptions
            self.updateTime=updateTime
            self.parent=parent
            self.identification=identification
            self.submodel_element=submodel_element
            self.id_short=id_short
            self.category=category
            self.description=description
            self.administration=administration
            self.semantic_id=semantic_id
            self.qualifier=qualifier
            self._kind= kind
            




#QosRequired Submodel

class QosRequiredN(SubmodelElementCollectionUnordered):
    def __init__(self, qfi, qos_profile, alternative_qos_profile, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        
        self.qfi = qfi
        self.qos_profile = qos_profile
        self.alternative_qos_profile = alternative_qos_profile
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        

class ListOfQosRequired(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfQosRequired = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_QosRequiredN(self, QosRequiredN):                                           
        self.listOfQosRequired.append(QosRequiredN)

    def remove_subscription(self, QosRequiredN):
        if QosRequiredN in self.listOfQosRequired:
            self.listOfQosRequired.remove(QosRequiredN)
            
        else:
            print("QosRequired did not find in the list")

class QosRequired(Submodel):
    def __init__(self, listOfQosRequired, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
            self.list_of_qos_required =listOfQosRequired
            self.parent=parent
            self.identification=identification
            self.submodel_element=submodel_element
            self.id_short=id_short
            self.category=category
            self.description=description
            self.administration=administration
            self.semantic_id=semantic_id
            self.qualifier=qualifier
            self._kind= kind
        
#Location Submodel
    
class LocationEventSubscription(Operation):
    def __init__(self, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
        

    def subscribe(self, qos_parameters, report_indicator, trigger_event):

        notification_message = None  
        return notification_message

class LocationResultsAndEvents(Operation):
    def __init__(self, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
        
    def process_event(self, event_notification_message):

        report = None  
        return report

class ListOfLocalizationSubscriptions(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.subscriptions = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_subscription(self, subscription):                                           
        self.subscriptions.append(subscription)
 
    def remove_subscription(self, subscription):
        if subscription in self.subscriptions:
            self.subscriptions.remove(subscription)
            
        else:
            print("Subscription did not find in the list")

class LocationUE(Submodel):
    def __init__(self, xPosition, yPosition,zPosition,speed, aceleration, lcs_qos_class, accuracy, response_time, list_of_subscriptions,locevsub,locresandev, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.xPosition = xPosition
        self.yPosition = yPosition
        self.zPosition = zPosition
        self.speed=speed
        self.aceleration=aceleration
        self.lcsQosClass = lcs_qos_class
        self.accuracy = accuracy
        self.responseTime = response_time
        self.listOfSubscriptions= list_of_subscriptions
        self.locationEventSubscription=locevsub
        self.locationResultsAndEvents=locresandev
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        
#Nameplate Submodel
class PhysicalAddress(SubmodelElementCollectionUnordered):
    def __init__(self, country_code, street, postal_code, city, state_county, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.countryCode=country_code
        self.street=street
        self.postalCode=postal_code
        self.city=city
        self.stateCounty=state_county
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class Nameplate(Submodel):
    def __init__(self,manufacturer_name,manufacturer_typ_name,physical_address, typ_class, serialno, chargeid,countryoforigin,yearofconstruction, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., 
                 id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.manufacturerName=manufacturer_name
        self.manufacturerTypName=manufacturer_typ_name
        self.physicalAddress=physical_address
        self.typClass=typ_class
        self.serialNo=serialno
        self.chargeId=chargeid
        self.countryOfOrigin= countryoforigin
        self.yearOfConstruction= yearofconstruction
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        


#Identification Submodel
class ContactInfo(SubmodelElementCollectionUnordered):
    def __init__(self,name,role,physical_address,email,url,phone, fax, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.name=name
        self.role=role
        self.physicalAddress= physical_address
        self.email=email
        self.url=url
        self.phone=phone
        self.fax=fax
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             


class Identification(Submodel):
    def __init__(self, manufacturer_name, manufacturer_id, manufacturer_idprovider, manufacturer_typid, manufacturer_typname, manufacturer_typdesc, suppliername, supplierid,supplierid_provider, supplier_typid, supplier_typname, supplier_typdesc, typclass,classystem,
                 seckeytyp, asset_id, instance_id, chargeid,seckeyinst, manufacturing_date, device_rev, software_rev, hardware_rev, ContactInfo, url, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.manufacturerName=manufacturer_name
        self.manufacturerId=manufacturer_id
        self.manufacturerIdProvider=manufacturer_idprovider
        self.manufacturerTypId= manufacturer_typid
        self.manufacturerTypName= manufacturer_typname
        self.manufacturerTypDescription=manufacturer_typdesc
        self.supplierName=suppliername
        self.supplierId=supplierid
        self.supplierIdProvider=supplierid_provider
        self.supplierTypId=supplier_typid
        self.supplierTypName=supplier_typname
        self.supplierTypDescription=supplier_typdesc
        self.typClass=supplier_typdesc
        self.classificationSystem=classystem
        self.secondaryKeyTyp=seckeytyp
        self.assetId=asset_id
        self.instanceId=instance_id
        self.chargeId=chargeid
        self.secondaryKeyInstance=seckeyinst
        self.manufacturingDate=manufacturing_date
        self.deviceRevision=device_rev
        self.softwareRevision=software_rev
        self.hardwareRevision=hardware_rev
        self.contactInfo=ContactInfo
        self.url=url
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        
                
#Documentation Submodel

class Documentation(Submodel):
    def __init__(self, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        




#Service Submodel
class Service(Submodel):
    def __init__(self, contact_info,identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.contactInfo=contact_info
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        

#TechnicalData Submodel
        
class GeneralInformation(SubmodelElementCollectionUnordered):
    def __init__(self, manufacturer_name, manufacturer_productdesignations,manufacturer_partnumber,manufacturer_ordercode, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.manufacturerName= manufacturer_name
        self.manufacturerProductDesignations=manufacturer_productdesignations
        self.manufacturerpartNumber=manufacturer_partnumber
        self.manufacturerOrdeCcode=manufacturer_ordercode
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class ProductClassificationItem(SubmodelElementCollectionUnordered):
    def __init__(self,prodclassys,classifsysvers,productclassid, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.productClassificationSystem= prodclassys
        self.classificationSystemVersion=classifsysvers
        self.productClassId=productclassid
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class ProductClassifications(SubmodelElementCollectionUnordered):
    def __init__(self, prodclassitem, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.productClassificationItem=prodclassitem
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             



class TechnicalProperties(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind             

class FurtherInformation(SubmodelElementCollectionUnordered):
    def __init__(self, txt_statement,validate, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.textStatement=txt_statement
        self.validDate=validate
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             


class TechnicalData(Submodel):
    def __init__(self,geninfo,prodclass,techprop,furtherinfo, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.generalInformation=geninfo
        self.productClassifications=prodclass
        self.technicalProperties=techprop
        self.furtherInformation=furtherinfo
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
                


#UE5GDataSheet
class UEChannelBandwidth(SubmodelElementCollectionUnordered):
    def __init__(self, maxtxbw, mingbandtxbwconfig,id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.maximumTransmissionBandwidth= maxtxbw
        self.minimumGuardbandAndTransmissionBandwidthConfiguration=mingbandtxbwconfig
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             


class OutputPowerDynamics(SubmodelElementCollectionUnordered):
    def __init__(self, minoutpower,txoffpower,id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.minimumOutputPower=minoutpower
        self.transmitOFFPower=txoffpower
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class TransmitterCharacteristics(SubmodelElementCollectionUnordered):
    def __init__(self,uemaxoutpower,outpowerdynamic, txsignalquality, outrfspecemissions, num_antenas, num_layers, maximumDataUplink, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.ueMaximumOutputPower= uemaxoutpower
        self.outputPowerDynamics= outpowerdynamic
        self.transmitSignalQuality= txsignalquality
        self.outputRFSpectrumEmissions= outrfspecemissions
        self.numberOfAntennas= num_antenas
        self.numberOfLayers=num_layers
        self.maximumDataUplink=maximumDataUplink
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class ReceiverCharacteristics(SubmodelElementCollectionUnordered):
    def __init__(self, refsenspowerlvl,maxinlvl,adjchannelselect,num_antenas, num_layers, maximumDataDownlink, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.referenceSensitivityPowerLevel=refsenspowerlvl
        self.maximumInputLevel=maxinlvl
        self.adjacentChannelSelectivity=adjchannelselect
        self.numberOfAntennas= num_antenas
        self.numberOfLayers=num_layers
        self.maximumDataDownlink=maximumDataDownlink
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class UE5GDataSheet(Submodel):
    def __init__(self,opbands,ue_channel_bw, duplex_mode, tx_characteristics, rx_characteristics, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.operatingBands=opbands
        self.ueChannelBandwidth=ue_channel_bw
        self.duplexMode=duplex_mode
        self.transmitterCharacteristics= tx_characteristics
        self.receiverCharacteristics=rx_characteristics
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

#SIMCard Submodel
class SIMCard(Submodel):
    def __init__(self,imsi,iccid,pin,spn,authenticationkey, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.imsi=imsi
        self.iccid=iccid
        self.pin=pin
        self.spn=spn
        self.authenticationKey=authenticationkey
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
                



#Ue5GIdentification Submodel
class Ue5GIdentification(Submodel):
    def __init__(self, pei,gpsi,authcertificate,certificatestatus, ipAddress, macAddress, imei, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.permanentEquipmentIdentifier=pei
        self.ueIdentityGPSI=gpsi
        self.authenticationCertificate=authcertificate
        self.certificateStatus=certificatestatus
        self.ipAddress=ipAddress
        self.macAddress=macAddress
        self.imei=imei
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        



#NETWORK AAS


#Network 5G Asset
class Network5G(Asset):
    def __init__(self, kind: AssetKind, identification: Identifier, id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, asset_identification_model: AASReference[Submodel] | None = None, bill_of_material: AASReference[Submodel] | None = None):
            super().__init__(kind, identification, id_short, category, description, parent, administration, asset_identification_model, bill_of_material)



#Network 5G AAS
class AASNetwork5G(AssetAdministrationShell):
    def __init__(self, asset_real,nameplate,identification_submodel,documentation,service, technicalData, npn5GNWIdentity, assetServiceRegistry, tsnCapabilities, network5GDataSheet, virtualsNetwork, connectivity, qosPerformance, location, dataAnalytics, asset: AASReference[Asset], identification: Identifier, id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, security: Security | None = None, submodel: Set[AASReference[Submodel]] | None = None, concept_dictionary: Iterable[ConceptDictionary] = ..., view: Iterable[View] = ..., derived_from: AASReference[AssetAdministrationShell] | None = None):
       self.listOfUeAAS = []
       self.asset_real=asset_real                               
       self.nameplate=nameplate                                 
       self.identification_submodel=identification_submodel     
       self.documentation=documentation                         
       self.service=service                                     
       self.technicalData=technicalData                         
       self.npn5GNWIdentity=npn5GNWIdentity                     
       self.assetServiceRegistry=assetServiceRegistry           
       self.tsnCapabilities=tsnCapabilities                     
       self.network5GDataSheet=network5GDataSheet               
       self.virtualsNetwork=virtualsNetwork                     
       self.connectivity=connectivity                           
       self.qosPerformance=qosPerformance                       
       self.location=location                                   
       self.dataAnalytics=dataAnalytics                         
       super().__init__(asset, identification, id_short, category, description, parent, administration, security, submodel, concept_dictionary, view, derived_from)        
     

    def add_ueAAS(self, ueAAS):
        self.listOfUeAAS.append(ueAAS)

    def remove_ueAAS(self, ueAAS):
        if ueAAS in self.listOfUeAAS:
            self.listOfUeAAS.remove(ueAAS)
            
        else:
            print("UE AAS did not find in the list")
       





#DataAnalytics Submodel
class DataAnalytics(Submodel):
    def __init__(self, analytics, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.analytics=analytics
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind  



#Location Submodel

 

class ListOfNetworkLocationSubscriptions(SubmodelElementCollectionUnordered):
    def __init__(self, events, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.subscriptions = []
        self.events=events
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

    def add_subscription(self, subscription):                                           
        self.subscriptions.append(subscription)
 
    def remove_subscription(self, subscription):
        if subscription in self.subscriptions:
            self.subscriptions.remove(subscription)
            
        else:
            print("Suscription did not find in the list")  


class ListOfNetworkLocationEvents(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.events = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
          

    def add_event(self, event):                                           
        self.events.append(event)
 
    def remove_event(self, event):
        if event in self.events:
            self.events.remove(event)
            
        else:
            print("Event did not find in the list")


class ConnectedUe(SubmodelElementCollectionUnordered):
    def __init__(self, xPosition, yPosition, zPosition, speed, aceleration, lcs_qos_class, accuracy, response_time, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.xPosition = xPosition
        self.yPosition = yPosition
        self.zPosition = zPosition
        self.speed=speed
        self.aceleration=aceleration
        self.lcsQosClass = lcs_qos_class
        self.accuracy = accuracy
        self.responseTime = response_time
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
         



class ListOfConnectedUes(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.connectedUes = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
         


    def add_ue(self, connectedUe):                                           
        self.connectedUes.append(connectedUe)
 
    def remove_ue(self, connectedUe):
        if connectedUe in self.connectedUes:
            self.connectedUes.remove(connectedUe)
            
        else:
            print("UE did not find in the list")


class SubscriptionRequestFunction(Operation):
    def __init__(self, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
        
    

class Location(Submodel):
    def __init__(self, listOfConnectedUes, subscriptionRequestFunction, listOfSubscriptions, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfConnectedUes= listOfConnectedUes
        self.subscriptionRequestFunction=subscriptionRequestFunction
        self.listOfSubscriptions=listOfSubscriptions
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        

#NPN5GNWIdentity Submodel
class NPN5GNWIdentity(Submodel):
    def __init__(self, plmnid,npnid, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.plmnID=plmnid
        self.npnID=npnid
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

#AssetServiceRegistry Submodel
class AssetServiceRegistry(Submodel):
    def __init__(self, assetService, integratorCompany,planningReferences,sla, coverageMap, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.assetService=assetService
        self.integratorCompany=integratorCompany
        self.planningReferences=planningReferences
        self.sla=sla
        self.coverageMap=coverageMap
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind




         

#The TechnicalData submodel is in the UE
        

#Network5GDataSheet Submodel
class InterfacesCapacityMbps(SubmodelElementCollectionUnordered):
    def __init__(self, n1, n2, n3, n4, n5, n6, rx, naf, npcf, nsmf, namf, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.n1=n1
        self.n2=n2
        self.n3=n3
        self.n4=n4
        self.n5=n5
        self.n6=n6
        self.rx=rx
        self.naf=naf
        self.npcf=npcf
        self.nsmf=nsmf
        self.namf=namf
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind





class ListOfUPF(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.upfList=[]
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

    def add_upf(self, upf):                                           
        self.upfList.append(upf)
 
    def remove_upf(self, upf):
        if upf in self.upfList:
            self.upfList.remove(upf)
            
        else:
            print("UPF did not find in the list")


class CoreNetwork(SubmodelElementCollectionUnordered):   
    def __init__(self, ipAddress, descriptionn, resources, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.ipAddress=ipAddress
        self.descriptionn= descriptionn
        self.resources=resources
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind



class ListOfCN(SubmodelElementCollectionUnordered):
    def __init__(self, upfList, af, pcf, smf, amf, nssf, lmf, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.upfList=upfList
        self.af=af
        self.pcf=pcf
        self.smf=smf
        self.amf=amf
        self.nssf=nssf
        self.lmf=lmf
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class Resources(SubmodelElementCollectionUnordered):    
    def __init__(self, position, virtualMachineID, storageMemory, ramMemory, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.position=position
        self.virtualMachineID=virtualMachineID
        self.storageMemory=storageMemory
        self.ramMemory=ramMemory
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class RANConnections(SubmodelElementCollectionUnordered):
    def __init__(self, upfByInterfaceN3, amfByInterfaceN2, ue, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.upfByInterfaceN3=upfByInterfaceN3
        self.amfByInterfaceN2=amfByInterfaceN2
        self.ue=ue
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class RanNodeN(SubmodelElementCollectionUnordered):
    def __init__(self, descriptionn, ipAddress, connections, gNBBasebandUnitResources, gNBRemoteRadioUnitResources, supportedSpectrum, transmissionPower, maximumTransmissionBandwidth, receiverSensitivity, transceiverTiming, numAntennas, numLayers, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.descriptionn=descriptionn
        self.ipAddress=ipAddress
        self.connections=connections
        self.gNBBasebandUnitResources=gNBBasebandUnitResources
        self.gNBRemoteRadioUnitResources=gNBRemoteRadioUnitResources
        self.supportedSpectrum=supportedSpectrum
        self.transmissionPower=transmissionPower
        self.maximumTransmissionBandwidth=maximumTransmissionBandwidth
        self.receiverSensitivity=receiverSensitivity
        self.transceiverTiming=transceiverTiming
        self.numAntennas=numAntennas
        self.numLayers=numLayers
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class ListOfRanNodes(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.ranNodes=[]
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


    def add_node(self, node):                                           
        self.ranNodes.append(node)
 
    def remove_ue(self, node):
        if node in self.ranNodes:
            self.ranNodes.remove(node)
            
        else:
            print("RAN node did not find in the list")


class NetworkTopology(SubmodelElementCollectionUnordered):
    def __init__(self, listOfRanNodes, listOfCN, interfacesCapacityMbps, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfRanNodes=listOfRanNodes
        self.listOfCN= listOfCN
        self.interfacesCapacityMbps=interfacesCapacityMbps
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class ModulationTypes(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class TransmissionModeCharacteristics(SubmodelElementCollectionUnordered):
    def __init__(self,  modulationTypes, maximumBitRate,id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.modulationTypes=modulationTypes
        self.maximumBitRate=maximumBitRate
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class Network5GDataSheet(Submodel):
    def __init__(self, releaseCompatibility3GPP, networkProtocols, coverageRange, supportedSpectrum, supportedTransmissionPower, transmissionModeCharacteristics, networkTopology, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.releaseCompatibility3GPP=releaseCompatibility3GPP
        self.networkProtocols=networkProtocols
        self.coverageRange=coverageRange
        self.supportedSpectrum=supportedSpectrum
        self.supportedTransmissionPower=supportedTransmissionPower
        self.transmissionModeCharacteristics=transmissionModeCharacteristics
        self.networkTopology=networkTopology
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


#TSNCapabilities Submodel

class TSCAI(SubmodelElementCollectionUnordered):
    def __init__(self, survivalTime, packetArrivalTime, periodicity, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.survivalTime=survivalTime
        self.packetArrivalTime=packetArrivalTime
        self.periodicity=periodicity
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class TsnFlowN(SubmodelElementCollectionUnordered):
    def __init__(self, destinationIP, destinationMacAddress, streamId, qosProfileAssociated, tsnTrafficClasses, tsnTrafficPriority, vlanID, vlanPriority, maximumLatency, reliability, port, tscai, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.destinationIP=destinationIP
        self.destinationMacAddress=destinationMacAddress
        self.streamId=streamId
        self.qosProfileAssociated=qosProfileAssociated
        self.tsnTrafficClasses=tsnTrafficClasses
        self.tsnTrafficPriority=tsnTrafficPriority
        self.vlanID=vlanID
        self.vlanPriority=vlanPriority
        self.maximumLatency=maximumLatency
        self.reliability=reliability
        self.port=port
        self.tscai=tscai
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind



class TsnFlowList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.tsnFlowList=[]
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        
    def add_tsnFlow(self, tsnFlow):                                           
        self.tsnFlowList.append(tsnFlow)
 
    def remove_tsnFlow(self, tsnFlow):
        if tsnFlow in self.tsnFlowList:
            self.tsnFlowList.remove(tsnFlow)
            
        else:
            print("TsnFlow did not find in the list")


class TimeSynchronizationStatus(SubmodelElementCollectionUnordered):
    def __init__(self,  synchronizationState, clockAccuracy, frequencyStability, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.synchronizationState=synchronizationState
        self.clockAccuracy=clockAccuracy
        self.frequencyStability=frequencyStability
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class ListOfPorts(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfPorts=[]
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        
    def add_port(self, port):                                           
        self.listOfPorts.append(port)
 
    def remove_port(self, port):
        if port in self.listOfPorts:
            self.listOfPorts.remove(port)
            
        else:
            print("Port did not find in the list")

class Parameters5GS(SubmodelElementCollectionUnordered):
    def __init__(self,  bridgeID, listOfPorts, system5GBridgeDelay, streamFilters, ueDsTtResidenceTime, propagationDelayPerPort, timeSynchronizationStatus, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.bridgeID=bridgeID
        self.listOfPorts=listOfPorts
        self.system5GBridgeDelay=system5GBridgeDelay
        self.streamFilters=streamFilters
        self.ueDsTtResidenceTime=ueDsTtResidenceTime
        self.propagationDelayPerPort=propagationDelayPerPort
        self.timeSynchronizationStatus=timeSynchronizationStatus
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class Bridge5GS(SubmodelElementCollectionUnordered):
    def __init__(self,  parameters5GS, tsnFlowList, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.parameters5GS=parameters5GS
        self.tsnFlowList=tsnFlowList
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class TSNCapabilities(Submodel):
    def __init__(self, bridge5GS, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.bridge5GS=bridge5GS
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


#VirtualsNetwork submodel
class SliceCommunicationAtributes(SubmodelElementCollectionUnordered):
    def __init__(self, radioSpectrum, availability, areaOfService, isolationLevel, maximumSupportedPacketSize, serviceCategory, networkSliceEnergyEficiency,  id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.radioSpectrum=radioSpectrum
        self.availability=availability
        self.areaOfService=areaOfService
        self.isolationLevel=isolationLevel
        self.maximumSupportedPacketSize=maximumSupportedPacketSize
        self.serviceCategory=serviceCategory
        self.networkSliceEnergyEficiency=networkSliceEnergyEficiency
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind




class SNSSAI(SubmodelElementCollectionUnordered):
    def __init__(self, sliceServiceType, sliceDifferenciator, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.sliceServiceType=sliceServiceType
        self.sliceDifferenciator=sliceDifferenciator
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind



class NetworkSliceN(SubmodelElementCollectionUnordered):
    def __init__(self,  snssai, sliceComputingCapability, ipAddress, sliceCommunicationAtributes,  id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.snssai=snssai
        self.sliceComputingCapability=sliceComputingCapability
        self.ipAddress=ipAddress
        self.sliceCommunicationAtributes=sliceCommunicationAtributes
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class ListOfNetworkSlices(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfNetworkSlices=[]
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        
    def add_networkSlice(self, networkSlice):                                           
        self.listOfNetworkSlices.append(networkSlice)
 
    def remove_networkSlice(self, networkSlice):
        if networkSlice in self.listOfNetworkSlices:
            self.listOfNetworkSlices.remove(networkSlice)
            
        else:
            print("NetworkSlice did not find in the list")


class ComputingCapability(SubmodelElementCollectionUnordered):  
    def __init__(self,  virtualMachineID, storageMemory, ramMemory, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.virtualMachineID=virtualMachineID
        self.storageMemory=storageMemory
        self.ramMemory=ramMemory
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind



class VirtualLANN(SubmodelElementCollectionUnordered):
    def __init__(self,  virtualLANId, vlanPriority, vlanTag, virtualLANComputingCapability, ipAddress, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.virtualLANId=virtualLANId
        self.vlanPriority=vlanPriority
        self.vlanTag=vlanTag
        self.virtualLANComputingCapability=virtualLANComputingCapability
        self.ipAddress=ipAddress
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class ListOfVirtualLANs(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfVirtualLANs=[]
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
        
        
    def add_virtualLAN(self, virtualLAN):                                           
        self.listOfVirtualLANs.append(virtualLAN)
 
    def remove_virtualLAN(self, virtualLAN):
        if virtualLAN in self.listOfVirtualLANs:
            self.listOfVirtualLANs.remove(virtualLAN)
            
        else:
            print("VirtualLAN did not find in the list")



class VirtualsNetwork(Submodel):
    def __init__(self, listOfVirtualLANs, listOfNetworkSlices, networkSliceReconfigurationFunction, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfVirtualLANs=listOfVirtualLANs
        self.listOfNetworkSlices=listOfNetworkSlices
        self.networkSliceReconfigurationFunction=networkSliceReconfigurationFunction
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind



#Connectivity Submodel
class QosMappingFunction(Operation):
    def __init__(self, qosRequested, qosGuaranteed, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.qosRequested=qosRequested
        self.qosGuaranteed=qosGuaranteed
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
        


class QosProfileRequested(SubmodelElementCollectionUnordered):                      
    def __init__(self, qosParameters, qosCharacteristics, alternativeQosProfiles, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        
        self.qosParameters=qosParameters
        self.qosCharacteristics=qosCharacteristics
        self.alternativeQosProfiles=alternativeQosProfiles
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class QosProfileGuaranteed(SubmodelElementCollectionUnordered):                      
    def __init__(self, qosParameters, qosCharacteristics, alternativeQosProfiles, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        
        self.qosParameters=qosParameters
        self.qosCharacteristics=qosCharacteristics
        self.alternativeQosProfiles=alternativeQosProfiles
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class QosFlowConnectivity(SubmodelElementCollectionUnordered):                      
    def __init__(self, qfi, qosProfileRequested, qosProfileGuaranteed, pduSessionID, pduSessionType, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qfi = qfi              
        self.qosProfileRequested=qosProfileRequested
        self.qosProfileGuaranteed=qosProfileGuaranteed
        self.pduSessionID=pduSessionID
        self.pduSessionType=pduSessionType
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class QosFlowConnectivityList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qosFlows = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_qos_flow(self, qos_flow):
        self.qosFlows.append(qos_flow)

    def remove_qos_flow(self, qos_flow):
        if qos_flow in self.qosFlows:
            self.qosFlows.remove(qos_flow)
            
        else:
            print("QosFlow did not find in the list")

class PDUSessionConnectivity(SubmodelElementCollectionUnordered):
    def __init__(self,IPAddress,ue, ddnDestintation, linkDirection, qos_flow_list, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.ipAddress=IPAddress 
        self.ue=ue
        self.ddnDestintation=ddnDestintation
        self.linkDirection=linkDirection
        self.qosFlowList = qos_flow_list
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class PDUSessionConnectivityList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.pduSessions = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_pdu_session(self, pdu_session):
        self.pduSessions.append(pdu_session)

    def remove_pdu_session(self, pdu_session):
        if pdu_session in self.pduSessions:
            self.pduSessions.remove(pdu_session)
            
        else:
            print("PDU session did not find in the list")



class UeID(SubmodelElementCollectionUnordered):
    def __init__(self,  pei, gpsi, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.pei=pei
        self.gpsi=gpsi
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class UeAttachedN(SubmodelElementCollectionUnordered):
    def __init__(self,  ueID, ueDescription, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.ueID=ueID
        self.ueDescription=ueDescription
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class UEAttachedList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.uesAttached = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_ue(self, ue):
        self.uesAttached.append(ue)

    def remove_ue(self, ue):
        if ue in self.uesAttached:
            self.uesAttached.remove(ue)
            
        else:
            print("UE did not find in the list")

class Connectivity(Submodel):
    def __init__(self, uesAttachedList, pduSessionList, qosMappingFunction, getRANConfiguration, setRANConfiguration, establishingConnectionsFunction, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.uesAttachedList=uesAttachedList
        self.pduSessionList=pduSessionList
        self.qosMappingFunction=qosMappingFunction
        self.getRANConfiguration=getRANConfiguration
        self.setRANConfiguration=setRANConfiguration
        self.establishingConnectionsFunction=establishingConnectionsFunction
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind





#QosPerformance       
class ListOfNetworkPerformanceSubscriptions(SubmodelElementCollectionUnordered):
    def __init__(self, events, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.subscriptions = []
        self.events=events
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

    def add_subscription(self, subscription):                                           
        self.subscriptions.append(subscription)
 
    def remove_subscription(self, subscription):
        if subscription in self.subscriptions:
            self.subscriptions.remove(subscription)
            
        else:
            print("Subscription did not find in the list")  


class ListOfNetworkPerformanceEvents(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.events = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
          

    def add_event(self, event):                                           
        self.events.append(event)
 
    def remove_event(self, event):
        if event in self.events:
            self.events.remove(event)
            
        else:
            print("Event did not find in the list")
        
class SubscriptionRequestFunction(Operation):
    def __init__(self, event, ue, parameters, result, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.event=event
        self.ue=ue
        self.parameters=parameters
        self.result=result
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind
        
class EstimatePacketTransmissionPerformanceFunction(Operation):
    def __init__(self, listOfUes, listOfMessageToTx, performance, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfUes=listOfUes
        self.listOfMessageToTx=listOfMessageToTx
        self.performance=performance
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind

        
           

class EstimateNetworkPerformanceFunction(Operation):
    def __init__(self, listOfUes, listOfMessageToTx, generalNetworkPerformanceParameters, id_short: str, input_variable: List[OperationVariable] | None = None, output_variable: List[OperationVariable] | None = None, in_output_variable: List[OperationVariable] | None = None, category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.listOfUes=listOfUes
        self.listOfMessageToTx=listOfMessageToTx
        self.generalNetworkPerformanceParameters=generalNetworkPerformanceParameters
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.input_variable=input_variable
        self.output_variable=output_variable
        self.in_output_variable=in_output_variable
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind=kind    


class BLER(SubmodelElementCollectionUnordered):
    def __init__(self, average, current, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.current=current
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class NetworkPowerConsumption(SubmodelElementCollectionUnordered):
    def __init__(self, average, current, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.current=current
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class GeneralNetworkKeyPerformanceIndicators(SubmodelElementCollectionUnordered):
    def __init__(self, performanceEventSubscription, performanceResultsAndEvents, areaTrafficCapacity, updateTime, droppedConnections, handoverSuccessRate, successRateOfConnectionRequest, serviceBitrate, dataThroughput, packetErrorRatio, bler, networkPowerConsumption, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.performanceEventSubscription=performanceEventSubscription
        self.performanceResultsAndEvents=performanceResultsAndEvents
        self.areaTrafficCapacity=areaTrafficCapacity
        self.updateTime=updateTime
        self.droppedConnections=droppedConnections
        self.handoverSuccessRate=handoverSuccessRate
        self.successRateOfConnectionRequest=successRateOfConnectionRequest
        self.serviceBitrate=serviceBitrate
        self.dataThroughput=dataThroughput
        self.packetErrorRatio=packetErrorRatio
        self.bler=bler
        self.networkPowerConsumption=networkPowerConsumption
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class Latency(SubmodelElementCollectionUnordered):
    def __init__(self, average, maximum, minimum, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.max=maximum
        self.min=minimum
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class ServiceBitRate(SubmodelElementCollectionUnordered):
    def __init__(self, average, maximum, minimum, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.max=maximum
        self.min=minimum
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class BLER(SubmodelElementCollectionUnordered):
    def __init__(self, average, maximum, minimum, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.max=maximum
        self.min=minimum
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class Throughput(SubmodelElementCollectionUnordered):
    def __init__(self, average, maximum, minimum, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.max=maximum
        self.min=minimum
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class DataThroughput(SubmodelElementCollectionUnordered):
    def __init__(self, average, maximum, minimum, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.max=maximum
        self.min=minimum
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class PacketErrorRatio(SubmodelElementCollectionUnordered):
    def __init__(self, average, maximum, minimum, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.average=average
        self.max=maximum
        self.min=minimum
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind

class NetworkSignalLevel(SubmodelElementCollectionUnordered):
    def __init__(self, sinr, rssi, rsrp,rsrq, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.sinr=sinr
        self.rssi=rssi
        self.rsrp=rsrp
        self.rsrq=rsrq
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class LogicalNetworkNPerformance(SubmodelElementCollectionUnordered):
    def __init__(self, actualBitRate, packetLossRate, bler, throughput, latency, reliability, capacityUtilization, signalLevel, id_short: str, value: Iterable[SubmodelElement] = ..., category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.actualBitRate=actualBitRate
        self.packetLossRate=packetLossRate
        self.bler=bler
        self.throughput=throughput
        self.latency=latency
        self.reliability=reliability
        self.capacityUtilization=capacityUtilization
        self.signalLevel=signalLevel
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


class LogicalNetworkList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.logicalNetworks = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_logicalNetwork(self, logicalNetwork):
        self.logicalNetworks.append(logicalNetwork)

    def remove_pdu_session(self, logicalNetwork):
        if logicalNetwork in self.logicalNetworks:
            self.logicalNetworks.remove(logicalNetwork)
            
        else:
            print("LogicalNetwork did not find in the list")



class QosFlowPerformance(SubmodelElementCollectionUnordered):                      
    def __init__(self, qfi, commservavai,commservrel, e2el, survivaltime, latency, servicebitrate,per,bler,updatetime,data_throughput, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qfi = qfi              
        self.communicationServiceAvailabilty=commservavai
        self.communicationServiceReliability=commservrel
        self.endToEndLatency=e2el
        self.updateTime=updatetime
        self.survivalTime=survivaltime
        self.latency=latency
        self.serviceBitRate=servicebitrate
        self.dataThroughput=data_throughput
        self.packetErrorRatio=per
        self.bler=bler
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class QosFlowPerformanceList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.qosFlows = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_qos_flow(self, qos_flow):
        self.qosFlows.append(qos_flow)

    def remove_qos_flow(self, qos_flow):
        if qos_flow in self.qosFlows:
            self.qosFlows.remove(qos_flow)
            
        else:
            print("QosFlow did not find in the list")

class PDUSessionPerformance(SubmodelElementCollectionUnordered):
    def __init__(self,IPAddress,qos_flow_list, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.ipAddress=IPAddress 
        self.qosFlowList = qos_flow_list
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

class PDUSessionPerformanceList(SubmodelElementCollectionUnordered):
    def __init__(self, id_short: str,
                 value: Iterable[SubmodelElement] = (), category: Optional[str] = None, description: Optional[base.LangStringSet] = None, parent: Optional[base.Namespace] = None,
                 semantic_id: Optional[base.Reference] = None, qualifier: Optional[Set[base.Constraint]] = None, kind: base.ModelingKind = base.ModelingKind.INSTANCE):
        self.pduSessions = []
        self.parent=parent
        self._id_short=id_short
        self.id_short=id_short
        self.value=value
        self.category=category
        self.description=description
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind
             

    def add_pdu_session(self, pdu_session):
        self.pduSessions.append(pdu_session)

    def remove_pdu_session(self, pdu_session):
        if pdu_session in self.pduSessions:
            self.pduSessions.remove(pdu_session)
            
        else:
            print("PDU session did not find in the list")


class QosPerformance(Submodel):
    def __init__(self, logicalNetworkList, parameterspertainingConnections, listOfSubscriptions, subscriptionRequestFunction, estimatePacketTransmissionPerformanceFunction, estimateNetworkPerformanceFunction, updateTime, identification: Identifier, submodel_element: Iterable[SubmodelElement] = ..., id_short: str = "", category: str | None = None, description: LangStringSet | None = None, parent: Namespace | None = None, administration: AdministrativeInformation | None = None, semantic_id: Reference | None = None, qualifier: Set[Constraint] | None = None, kind: ModelingKind = base.ModelingKind.INSTANCE):
        self.logicalNetworkList=logicalNetworkList
        self.parameterspertainingConnections=parameterspertainingConnections
        self.listOfSubscriptions=listOfSubscriptions
        self.subscriptionRequestFunction=subscriptionRequestFunction
        self.estimatePacketTransmissionPerformanceFunction=estimatePacketTransmissionPerformanceFunction
        self.estimateNetworkPerformanceFunction=estimateNetworkPerformanceFunction
        self.updateTime=updateTime
        self.parent=parent
        self.identification=identification
        self.submodel_element=submodel_element
        self.id_short=id_short
        self.category=category
        self.description=description
        self.administration=administration
        self.semantic_id=semantic_id
        self.qualifier=qualifier
        self._kind= kind


    
        
    