#!/usr/bin/python2.7

import os
import re

all_event_list = []; # insert all tracepoint event related with this script
irq_dic = {}; # key is cpu and value is a list which stacks irqs
              # which raise NET_RX softirq
net_rx_dic = {}; # key is cpu and value include time of NET_RX softirq-entry
		 # and a list which stacks receive
receive_hunk_list = []; # a list which include a sequence of receive events
rx_skb_list = []; # received packet list for matching
		       # skb_copy_datagram_iovec

buffer_budget = 65536; # the budget of rx_skb_list, tx_queue_list and
		       # tx_xmit_list
of_count_rx_skb_list = 0; # overflow count

tx_queue_list = []; # list of packets which pass through dev_queue_xmit
of_count_tx_queue_list = 0; # overflow count

tx_xmit_list = [];  # list of packets which pass through dev_hard_start_xmit
of_count_tx_xmit_list = 0; # overflow count

tx_free_list = [];  # list of packets which is freed

# options
show_tx = 0;
show_rx = 0;
dev = 0; # store a name of device specified by option "dev="
debug = 0;

# indices of event_info tuple
EINFO_IDX_NAME=   0
EINFO_IDX_CONTEXT=1
EINFO_IDX_CPU=    2
EINFO_IDX_TIME=   3
EINFO_IDX_PID=    4
EINFO_IDX_COMM=   5


trace_event_type = [
"softirq_exit",
"softirq_entry",
"softirq_raise",
"irq_handler_entry",
"irq_handler_exit",
"napi_poll",
"netif_receive_skb",
"net_netif_rx",
"skb_copy_datagram_iovec",
"net_dev_queue",
"net_dev_xmit",
"kfree_skb",
"consume_skb"
]


FUTEX_WAIT = 0
FUTEX_WAKE = 1
FUTEX_PRIVATE_FLAG = 128
FUTEX_CLOCK_REALTIME = 256
FUTEX_CMD_MASK = ~(FUTEX_PRIVATE_FLAG | FUTEX_CLOCK_REALTIME)

NSECS_PER_SEC    = 1000000
# Format for displaying rx packet processing
PF_IRQ_ENTRY= "  irq_entry(+%.3fmsec irq=%d:%s)"
PF_SOFT_ENTRY="  softirq_entry(+%.3fmsec)"
PF_NAPI_POLL= "  napi_poll_exit(+%.3fmsec %s)"
PF_JOINT=     "         |"
PF_WJOINT=    "         |            |"
PF_NET_RECV=  "         |---netif_receive_skb(+%.3fmsec skb=%x len=%d)"
PF_NET_RX=    "         |---netif_rx(+%.3fmsec skb=%x)"
PF_CPY_DGRAM= "         |      skb_copy_datagram_iovec(+%.3fmsec %d:%s)"
PF_KFREE_SKB= "         |      kfree_skb(+%.3fmsec location=%x)"
PF_CONS_SKB=  "         |      consume_skb(+%.3fmsec)"

# Calculate a time interval(msec) from src(nsec) to dst(nsec)
def diff_msec(src, dst):
    return (dst - src) / 1000000.0

def avg(total, n):
    return total / n

def nsecs(secs, nsecs):
    return secs * NSECS_PER_SEC + nsecs

def nsecs_secs(nsecs):
    return nsecs / NSECS_PER_SEC

def nsecs_nsecs(nsecs):
    return nsecs % NSECS_PER_SEC

def nsecs_str(nsecs):
    str = "%5u.%09u" % (nsecs_secs(nsecs), nsecs_nsecs(nsecs)),
    return str

def transfer_trace_to_event(event_str):
    ""
    event_info=re.split(r" +", event_str)
    for name in trace_event_type:
        evnet_cmp=name+":"
        #if event_name in event:
        if evnet_cmp in event_info:
            if name == 'softirq_exit':
                    handle_irq_softirq_exit(event_info)
            elif name == 'softirq_entry':
                    handle_irq_softirq_entry(event_info)
                    pass
            elif name == 'softirq_raise':
                    handle_irq_softirq_raise(event_info)
                    pass
            elif name == 'irq_handler_entry':
                    handle_irq_handler_entry(event_info)
                    pass
            elif name == 'irq_handler_exit':
                    handle_irq_handler_exit(event_info)
                    pass
            elif name == 'napi_poll':
                    handle_napi_poll(event_info)
                    pass
            elif name == 'netif_receive_skb':
                    handle_netif_receive_skb(event_info)
                    pass
            elif name == 'net_netif_rx':
                    handle_netif_rx(event_info)
            elif name == 'skb_copy_datagram_iovec':
                    handle_skb_copy_datagram_iovec(event_info)
                    pass
            elif name == 'net_dev_queue':
                    handle_net_dev_queue(event_info)
                    pass
            elif name == 'net_dev_xmit':
                    handle_net_dev_xmit(event_info)
                    pass
            elif name == 'kfree_skb':
                    handle_kfree_skb(event_info)
                    pass
            elif name == 'consume_skb':
                    handle_consume_skb(event_info)
                    pass

def handle_consume_skb(event_info):
    #(name, context, cpu, time, pid, comm, skbaddr) = event_info
    (var1, pid, cpu, var2,  time, comm, skbaddr) = event_info
    cpu=cpu[1:-1]
    time=time[:-1]
    comm=comm[:-1]
    skbaddr=skbaddr[8:-1]
    print pid, cpu, time, comm, skbaddr
    for i in range(len(tx_xmit_list)):
            skb = tx_xmit_list[i]
            if skb['skbaddr'] == skbaddr:
                    skb['free_t'] = time
                    tx_free_list.append(skb)
                    del tx_xmit_list[i]
                    return

def handle_kfree_skb(event_info):
    #(name, context, cpu, time, pid, comm,
    #        skbaddr, protocol, location) = event_info
    print event_info
    (var1, pid, cpu, var2,  time, comm, skbaddr, protocol, location) = event_info
    cpu=cpu[1:-1]
    time=time[:-1]
    comm=comm[:-1]
    skbaddr=skbaddr[8:]
    print pid, cpu, time, comm, skbaddr, protocol, location[:-1]
    for i in range(len(tx_queue_list)):
            skb = tx_queue_list[i]
            if skb['skbaddr'] == skbaddr:
                    del tx_queue_list[i]
                    return
    for i in range(len(tx_xmit_list)):
            skb = tx_xmit_list[i]
            if skb['skbaddr'] == skbaddr:
                    skb['free_t'] = time
                    tx_free_list.append(skb)
                    del tx_xmit_list[i]
                    return
    for i in range(len(rx_skb_list)):
            rec_data = rx_skb_list[i]
            if rec_data['skbaddr'] == skbaddr:
                    rec_data.update({'handle':"kfree_skb",
                                    'comm':comm, 'pid':pid, 'comm_t':time})
                    del rx_skb_list[i]
                    return

def handle_skb_copy_datagram_iovec(event_info):
    (var1, pid, cpu, var2,  time, comm, skbaddr, skblen) = event_info
    cpu=cpu[1:-1]
    time=time[:-1]
    comm=comm[:-1]
    skbaddr=skbaddr[8:]
    skblen=skblen[4:-1]
    print pid, cpu, time, comm, skbaddr, skblen
    for i in range(len(rx_skb_list)):
            rec_data = rx_skb_list[i]
            if skbaddr == rec_data['skbaddr']:
                    rec_data.update({'handle':"skb_copy_datagram_iovec",
                                    'comm':comm, 'pid':pid, 'comm_t':time})
                    del rx_skb_list[i]
                    return

def handle_netif_rx(event_info):
    (name, context, cpu, time, pid, comm,
            skbaddr, skblen, dev_name) = event_info
    if cpu not in irq_dic.keys() \
    or len(irq_dic[cpu]) == 0:
            return
    irq_record = irq_dic[cpu].pop()
    if 'event_list' in irq_record.keys():
            irq_event_list = irq_record['event_list']
    else:
            irq_event_list = []
    irq_event_list.append({'time':time, 'event':'netif_rx',
            'skbaddr':skbaddr, 'skblen':skblen, 'dev_name':dev_name})
    irq_record.update({'event_list':irq_event_list})
    irq_dic[cpu].append(irq_record)

def handle_netif_receive_skb(event_info):
    global of_count_rx_skb_list

    (var1, pid, cpu, var2,  time, comm, dev_name, skbaddr, skblen) = event_info
    cpu=cpu[1:-1]
    time=time[:-1]
    comm=comm[:-1]
    dev_name=dev_name[4:]
    skbaddr=skbaddr[8:]
    skblen=skblen[4:-1]
    print pid, cpu, time, comm, dev_name, skbaddr, skblen

    if cpu in net_rx_dic.keys():
            rec_data = {'event_name':'netif_receive_skb',
                        'event_t':time, 'skbaddr':skbaddr, 'len':skblen}
            event_list = net_rx_dic[cpu]['event_list']
            event_list.append(rec_data)
            rx_skb_list.insert(0, rec_data)
            if len(rx_skb_list) > buffer_budget:
                    rx_skb_list.pop()
                    of_count_rx_skb_list += 1
def handle_napi_poll(event_info):
    pid=event_info[1]
    cpu=event_info[2][1:-1]
    time=event_info[4][:-1]
    comm=event_info[5][:-1]
    dev_name=event_info[14][:-1]
    print pid, cpu, time, comm, dev_name
    if cpu in net_rx_dic.keys():
            event_list = net_rx_dic[cpu]['event_list']
            rec_data = {'event_name':'napi_poll',
                            'dev':dev_name, 'event_t':time}
            event_list.append(rec_data)

def handle_irq_softirq_raise(event_info):
    (var1, pid, cpu, var2, time, comm, irq, irq_name) = event_info
    cpu=cpu[1:-1]
    comm=comm[:-1]
    time=time[:-1]
    irq=irq[4:]
    irq_name=irq_name[8:-2]
    print pid, cpu, time, comm, irq, irq_name
    if cpu not in irq_dic.keys() \
    or len(irq_dic[cpu]) == 0:
            return
    irq_record = irq_dic[cpu].pop()
    if 'event_list' in irq_record.keys():
            irq_event_list = irq_record['event_list']
    else:
            irq_event_list = []
    irq_event_list.append({'time':time, 'event':'sirq_raise'})
    irq_record.update({'event_list':irq_event_list})
    irq_dic[cpu].append(irq_record)

def handle_irq_softirq_entry(event_info):
    #(name, context, cpu, time, pid, comm, vec) = event_info
    (var1, pid, cpu, var2, time, comm, irq, irq_name) = event_info
    cpu=cpu[1:-1]
    comm=comm[:-1]
    time=time[:-1]
    irq=irq[4:]
    irq_name=irq_name[8:-2]
    print pid, cpu, time, comm, irq, irq_name
    net_rx_dic[cpu] = {'sirq_ent_t':time, 'event_list':[]}

def handle_irq_softirq_exit(event_info):
    irq_list = []
    event_list = 0

    (var1, pid, cpu, var2, time, comm, irq, irq_name) = event_info
    cpu=cpu[1:-1]
    comm=comm[:-1]
    time=time[:-1]
    irq=irq[4:]
    irq_name=irq_name[8:-2]
    print pid, cpu, time, comm, irq, irq_name

    if cpu in irq_dic.keys():
            irq_list = irq_dic[cpu]
            del irq_dic[cpu]
    if cpu in net_rx_dic.keys():
            sirq_ent_t = net_rx_dic[cpu]['sirq_ent_t']
            event_list = net_rx_dic[cpu]['event_list']
            del net_rx_dic[cpu]
    if irq_list == [] or event_list == 0:
            return
    rec_data = {'sirq_ent_t':sirq_ent_t, 'sirq_ext_t':time,
                'irq_list':irq_list, 'event_list':event_list}
    # merge information realted to a NET_RX softirq
    receive_hunk_list.append(rec_data)

def handle_irq_handler_entry(event_info):
    #['', '<idle>-0', '[000]', 'd.h.', '512351.312182:', 'irq_handler_entry:', 'irq=76', 'name=eth0-tx-0\n']
    (var1, pid, cpu, var2, time, comm, irq, irq_name) = event_info
    cpu=cpu[1:-1]
    comm=comm[:-1]
    time=time[:-1]
    irq=irq[4:]
    irq_name=irq_name[5:-1]
    print pid, cpu, time, comm, irq, irq_name
    if cpu not in irq_dic.keys():
            irq_dic[cpu] = []
    irq_record = {'irq':irq, 'name':irq_name, 'cpu':cpu, 'irq_ent_t':time}
    irq_dic[cpu].append(irq_record)

def handle_irq_handler_exit(event_info):
    (var1, pid, cpu, var2, time, comm, irq, irq_name) = event_info
    cpu=cpu[1:-1]
    comm=comm[:-1]
    time=time[:-1]
    irq=irq[4:]
    irq_name=irq_name[4:-1]
    print pid, cpu, time, comm, irq, irq_name
    if cpu not in irq_dic.keys():
            return
    irq_record = irq_dic[cpu].pop()
    if irq != irq_record['irq']:
            return
    irq_record.update({'irq_ext_t':time})
    # if an irq doesn't include NET_RX softirq, drop.
    if 'event_list' in irq_record.keys():
            irq_dic[cpu].append(irq_record)

def handle_net_dev_queue(event_info):
    global of_count_tx_queue_list

    #['', '<...>-105788', '[001]', '..s.', '155386.949286:', 'net_dev_queue:', 'dev=eth0', 'skbaddr=ffff880fddb308f8', 'len=102\n']
    (var1, pid, cpu, var2,  time, comm, dev_name, skbaddr, skblen) = event_info
    cpu=cpu[1:-1]
    time=time[:-1]
    comm=comm[:-1]
    dev_name=dev_name[4:]
    skbaddr=skbaddr[8:]
    skblen=skblen[4:-1]
    #print cpu, pid, time, comm, dev_name, skbaddr, skblen
    skb = {'dev':dev_name, 'skbaddr':skbaddr, 'len':skblen, 'queue_t':time}
    tx_queue_list.insert(0, skb)
    if len(tx_queue_list) > buffer_budget:
            tx_queue_list.pop()
            of_count_tx_queue_list += 1

def handle_net_dev_xmit(event_info):
    global of_count_tx_xmit_list

    (var1, pid, cpu, var2,  time, comm, dev_name,
            skbaddr, skblen, rc) = event_info
    if rc == 0: # NETDEV_TX_OK
            for i in range(len(tx_queue_list)):
                    skb = tx_queue_list[i]
                    if skb['skbaddr'] == skbaddr:
                            skb['xmit_t'] = time
                            tx_xmit_list.insert(0, skb)
                            del tx_queue_list[i]
                            if len(tx_xmit_list) > buffer_budget:
                                    tx_xmit_list.pop()
                                    of_count_tx_xmit_list += 1
                            return

def print_receive(hunk):
    show_hunk = 0
    irq_list = hunk['irq_list']
    cpu = int(irq_list[0]['cpu'])
    base_t = int(irq_list[0]['irq_ent_t'].replace(".",""))
    # check if this hunk should be showed
    if dev != 0:
            for i in range(len(irq_list)):
                    if irq_list[i]['name'].find(dev) >= 0:
                            show_hunk = 1
                            break
    else:
            show_hunk = 1
    if show_hunk == 0:
            return

    print "%d.%06dsec cpu=%d" % \
            (nsecs_secs(base_t), nsecs_nsecs(base_t)/1000, cpu)
    for i in range(len(irq_list)):
            print PF_IRQ_ENTRY % \
                    (diff_msec(base_t, irq_list[i]['irq_ent_t']),
                    irq_list[i]['irq'], irq_list[i]['name'])
            print PF_JOINT
            irq_event_list = irq_list[i]['event_list']
            for j in range(len(irq_event_list)):
                    irq_event = irq_event_list[j]
                    if irq_event['event'] == 'netif_rx':
                            print PF_NET_RX % \
                                    (diff_msec(base_t, irq_event['time']),
                                    irq_event['skbaddr'])
                            print PF_JOINT
    print PF_SOFT_ENTRY % \
            diff_msec(base_t, hunk['sirq_ent_t'])
    print PF_JOINT
    event_list = hunk['event_list']
    for i in range(len(event_list)):
            event = event_list[i]
            if event['event_name'] == 'napi_poll':
                    print PF_NAPI_POLL % \
                        (diff_msec(base_t, event['event_t']), event['dev'])
                    if i == len(event_list) - 1:
                            print ""
                    else:
                            print PF_JOINT
            else:
                    print PF_NET_RECV % \
                        (diff_msec(base_t, event['event_t']), event['skbaddr'],
                            event['len'])
                    if 'comm' in event.keys():
                            print PF_WJOINT
                            print PF_CPY_DGRAM % \
                                    (diff_msec(base_t, event['comm_t']),
                                    event['pid'], event['comm'])
                    elif 'handle' in event.keys():
                            print PF_WJOINT
                            if event['handle'] == "kfree_skb":
                                    print PF_KFREE_SKB % \
                                            (diff_msec(base_t,
                                            event['comm_t']),
                                            event['location'])
                            elif event['handle'] == "consume_skb":
                                    print PF_CONS_SKB % \
                                            diff_msec(base_t,
                                                    event['comm_t'])
                    print PF_JOINT
    exit()

if 1:
    trace_fd=open("/sys/kernel/debug/tracing/trace", "r")
    buf=trace_fd.readlines()
    if buf:
        for line in buf:
            transfer_trace_to_event(line)

for i in range(len(receive_hunk_list)):
        print_receive(receive_hunk_list[i])
#while 1:
#    trace_fd=open("/sys/kernel/debug/tracing/trace_pipe", "r")
#    buf=trace_fd.readlines(2000)

#    if buf:
#        for line in buf:
#            transfer_trace_to_event(line)

