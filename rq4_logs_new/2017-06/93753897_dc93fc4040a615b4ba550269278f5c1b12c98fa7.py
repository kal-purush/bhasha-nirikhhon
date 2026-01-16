# Copyright (C) 2017 Eitan Geiger and Sebastian Scheinkman
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
from Jess.Endpoint.PolicyCommitMechanism import PolicyCommitMechanism
from Jess.Policy.firewall_rule import FWACTION


class IPTablesCommitMechanism(PolicyCommitMechanism):
    @staticmethod
    def _parse_protocol(protocol):
        # TODO: what about other protocols? ICMP, etc.
        proto = protocol.split('/')[0]
        if proto.lower() in ['icmp']:
            return proto.lower()
        elif proto in ['tcp', 'udp']:
            try:
                return "{proto} --dport {port}".format(proto=proto, port=protocol.split('/')[1])
            except IndexError:
                try:
                    return "{proto} --dport {port}".format(proto='tcp', port=protocol)
                except ValueError:
                    return "{proto}".format(proto=proto)

    @staticmethod
    def _add_rule(formal_rule, chain):
        action = "iptables -{insert_or_append} {chain} --source {source} --destinaiton {destination} " \
                 "-p {protocol_args} -j {action}".format(
                    insert_or_append='I' if formal_rule.action == FWACTION.ACCEPT else 'A',
                    chain=chain,
                    source=formal_rule.source,
                    destination=formal_rule.destination,
                    protocol_args=IPTablesCommitMechanism._parse_protocol(formal_rule.protocol),
                    action=formal_rule.s_action()
        )
        return action

    def add_incoming_rule(self, formal_rule):
        action = IPTablesCommitMechanism._add_rule(formal_rule=formal_rule, chain='INPUT')
        super(IPTablesCommitMechanism, self).add_incoming_rule(formal_rule=formal_rule)
        return action

    def add_outgoing_rule(self, formal_rule):
        action = IPTablesCommitMechanism._add_rule(formal_rule=formal_rule, chain='OUTPUT')
        super(IPTablesCommitMechanism, self).add_outgoing_rule(formal_rule=formal_rule)
        return action

    def __repr__(self):
        representation = ""
        for chain, rules in [["OUTPUT", self.outgoing_rules],
                            ["FORWARD", self.forwarding_rules],
                            ["INPUT", self.incoming_rules]]:
            representation += "Chain {chain}\n".format(chain=chain)
            for rule in rules:
                representation += "{action}\t{protocol}\t--\t{source}\t{destination}\t{args}\n".format(
                    action=rule.s_action(),
                    protocol=rule.protocol,
                    source=rule.source,
                    destination=rule.destination,
                    args=IPTablesCommitMechanism._parse_protocol(rule.protocol)
                )
            representation += "\n"
        return representation

    def __str__(self):
        return self.__repr__()