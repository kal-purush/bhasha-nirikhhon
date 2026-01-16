import csv
import matplotlib.pyplot as plt
import numpy as np

pred_str = "Predecessor\n"
ins_str = "Insert\n"
alpha_0_str = r'$\alpha=0$'
alpha_1_str = r'$\alpha=1$'
point_size = 400

def list_sum(a, b):
    c = [0] * len(a)
    for i in range(len(a)):
        c[i] = a[i] + b[i]
    return c

def divlist(a, b):
    return [i/b for i in a]

all_tests = []
with open('result.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    columns = reader.fieldnames
    for row in reader:
        all_tests.append(row)

def find_result(idx_type, skew, op_type, batch_size, target):
    for test in all_tests:
        if test["idx_type"] == idx_type and test["skew"] == str(skew) and test["op_type"] == op_type and test["batch_size"] == str(batch_size):
            if test[target] == "" or test[target] == "nan" :
                continue
            assert(test[target] != None)
            return test[target]
    if idx_type == "range_partitioning" and op_type == "micro_insert" and target == "throughput":
        return 10000.0
    assert(False)
    # return 10000


def throughput_over_bias_between_pim_and_range():
    # Get
    width = 0.35
    comm_labels_ = ["", "0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    comm_labels = ["0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    pim_tree_time = [find_result("pim_tree", i, "micro_get", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    partitioned_time = [find_result("range_partitioning", i, "micro_get", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    # pim_tree_time = [2.63848, 2.83664, 2.78207, 2.77227, 2.61916, 2.75306, 2.62678]
    # partitioned_time = [1.97127, 3.32832, 5.831623, 12.707024, 30.4211, 61.359808, 112.850589]
    pim_tree = list(map(lambda x:100.0/x, pim_tree_time))
    partitioned = list(map(lambda x:100.0/x, partitioned_time))
    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(len(comm_labels))
    x1 = x - 0.5 * width
    x2 = x + 0.5 * width

    rects1 = ax.bar(x1, pim_tree, width, label="PIM-Tree", color='khaki', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, partitioned, width, label="Partitioned Skip List", color='cornflowerblue', edgecolor='black', zorder=3)
    ax.set_ylabel('Performance (Mop/s)', fontsize=40)
    ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels_, fontsize=25)
    plt.yticks(fontsize=30)
    ax.legend(fontsize=30, bbox_to_anchor=(0.41, -0.18))

    prop = [i/j for i, j in zip(pim_tree, partitioned)]
    ax2 = ax.twinx()
    ax2.plot(x, prop, label="Improvements of PIM-Tree\n over Partitioned Skip List",
        color='blue', ms=10, mfc='gold', lw=3, marker='o'
    )
    ax2.set_ylabel("Performance Improvement", fontsize=30)
    ax2.semilogy()
    plt.yticks(fontsize=30)
    ax2.legend(fontsize=30, bbox_to_anchor=(1.05, -0.19))
    for aa,bb in zip(x, prop):
        plt.text(aa-0.1, bb*1.1, '%.2f' % bb, ha='center', va= 'bottom',
            weight='demibold', fontsize=30
        )
    fig.tight_layout()
    plt.savefig('get.pdf')
    plt.show()

    # Predecessor
    width = 0.35
    comm_labels_ = ["", "0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    comm_labels = ["0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    pim_tree_time = [find_result("pim_tree", i, "micro_predecessor", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    partitioned_time = [find_result("range_partitioning", i, "micro_predecessor", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    # pim_tree_time = [7.01398, 7.54845, 8.29852, 8.10471, 8.0403, 8.01301, 7.74178]
    # partitioned_time = [2.926345, 5.726836, 15.3712, 41.294035, 106.678085, 227.026608, 427.351839]

    pim_tree = list(map(lambda x:100.0/x, pim_tree_time))
    partitioned = list(map(lambda x:100.0/x, partitioned_time))
    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(len(comm_labels))
    x1 = x - 0.5 * width
    x2 = x + 0.5 * width

    rects1 = ax.bar(x1, pim_tree, width, label="PIM-Tree", color='khaki', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, partitioned, width, label="Partitioned Skip List", color='cornflowerblue', edgecolor='black', zorder=3)
    ax.set_ylabel('Performance (Mop/s)', fontsize=40)
    ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels_, fontsize=25)
    plt.yticks(fontsize=30)
    ax.legend(fontsize=30, bbox_to_anchor=(0.41, -0.18))

    prop = [i/j for i, j in zip(pim_tree, partitioned)]
    ax2 = ax.twinx()
    ax2.plot(x, prop, label="Improvements of PIM-Tree\n over Partitioned Skip List",
        color='blue', ms=10, mfc='gold', lw=3, marker='o'
    )
    ax2.set_ylabel("Performance Improvement", fontsize=30)
    ax2.semilogy()
    plt.yticks(fontsize=30)
    ax2.legend(fontsize=30, bbox_to_anchor=(1.05, -0.19))
    for aa,bb in zip(x, prop):
        plt.text(aa-0.1, bb*1.1, '%.2f' % bb, ha='center', va= 'bottom',
            weight='demibold', fontsize=30
        )
    fig.tight_layout()
    plt.savefig('predecessor.pdf')
    plt.show()


    # Insert
    width = 0.35
    comm_labels_ = ["", "0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    comm_labels = ["0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    pim_tree_time = [find_result("pim_tree", i, "micro_insert", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    partitioned_time = [find_result("range_partitioning", i, "micro_insert", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    # pim_tree_time = [21.7865, 22.1249, 22.8191, 22.9841, 23.3308, 22.1934, 20.9148]
    # partitioned_time = [3.744016, 6.848397, 19.276130, 55.172107, 142.772519, 10000, 10000]
    pim_tree = list(map(lambda x:100.0/x, pim_tree_time))
    partitioned = list(map(lambda x:100.0/x, partitioned_time))
    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(len(comm_labels))
    x1 = x - 0.5 * width
    x2 = x + 0.5 * width

    rects1 = ax.bar(x1, pim_tree, width, label="PIM-Tree", color='khaki', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, partitioned, width, label="Partitioned Skip List", color='cornflowerblue', edgecolor='black', zorder=3)
    ax.set_ylabel('Performance (Mop/s)', fontsize=40)
    ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels_, fontsize=25)
    plt.yticks(fontsize=30)
    ax.legend(fontsize=30, bbox_to_anchor=(0.4, -0.18))

    ax2 = ax.twinx()
    prop = [i/j for i, j in zip(pim_tree, partitioned)]
    infinite_s = 12
    prop[5] = infinite_s
    prop[6] = infinite_s
    ax2.plot(x, prop, label="Improvements of PIM-Tree\n over Partitioned Skip List",
        color='blue', ms=10, mfc='gold', lw=3, marker='o'
    )
    for aa,bb in zip(x[0:5], prop[0:5]):
        plt.text(aa-0.1, bb*1.1, '%.2f' % bb, ha='center', va= 'bottom',
            weight='demibold', fontsize=30
        )
    prop = [infinite_s, infinite_s]
    ax2.plot(x[5:], prop, color='red', ms=30, mfc='red', lw=4, marker='X')
    for aa,bb in zip(x[6:], prop[1:]):
        plt.text(aa-0.5, bb*1.1, "Crash", ha='center', va= 'bottom',
            weight='demibold', fontsize=25
        )
    ax2.set_ylabel("Performance Improvement", fontsize=30)
    ax2.semilogy()
    plt.yticks(fontsize=30)
    ax2.legend(fontsize=30, bbox_to_anchor=(1.06, -0.19))
    fig.tight_layout()
    plt.savefig('insert.pdf')
    plt.show()


    # Delete
    width = 0.35
    comm_labels_ = ["", "0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    comm_labels = ["0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    pim_tree_time = [find_result("pim_tree", i, "micro_delete", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    partitioned_time = [find_result("range_partitioning", i, "micro_delete", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    # pim_tree_time = [21.6145, 19.9634, 22.0515, 21.1322, 22.4551, 21.9978, 22.1464]
    # partitioned_time = [1.727932, 2.634378, 5.874225, 15.380320, 31.333313, 55.941512, 80.489478]
    pim_tree = list(map(lambda x:100.0/x, pim_tree_time))
    partitioned = list(map(lambda x:100.0/x, partitioned_time))
    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(len(comm_labels))
    x1 = x - 0.5 * width
    x2 = x + 0.5 * width

    rects1 = ax.bar(x1, pim_tree, width, label="PIM-Tree", color='khaki', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, partitioned, width, label="Partitioned Skip List", color='cornflowerblue', edgecolor='black', zorder=3)
    ax.set_ylabel('Performance (Mop/s)', fontsize=40)
    ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels_, fontsize=25)
    plt.yticks(fontsize=30)
    ax.legend(fontsize=30, bbox_to_anchor=(0.4, -0.18))

    prop = [i/j for i, j in zip(pim_tree, partitioned)]
    ax2 = ax.twinx()
    ax2.plot(x, prop, label="Improvements of PIM-Tree\n over Partitioned Skip List",
        color='blue', ms=10, mfc='gold', lw=3, marker='o'
    )
    ax2.set_ylabel("Performance Improvement", fontsize=30)
    ax2.semilogy()
    plt.yticks(fontsize=30)
    ax2.legend(fontsize=30, bbox_to_anchor=(1.06, -0.19))
    for aa,bb in zip(x, prop):
        plt.text(aa-0.1, bb*1.1, '%.2f' % bb, ha='center', va= 'bottom',
            weight='demibold', fontsize=30
        )
    fig.tight_layout()
    plt.savefig('delete.pdf')
    plt.show()


    # Scan
    width = 0.35
    comm_labels_ = ["", "0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    comm_labels = ["0", "0.2", "0.4", "0.6", "0.8", "1", "1.2"]
    pim_tree_time = [find_result("pim_tree", i, "micro_scan", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    partitioned_time = [find_result("range_partitioning", i, "micro_scan", 1000000, "throughput") for i in np.arange(0.0, 1.4, 0.2)]
    # pim_tree_time = [5.02365, 4.9659, 4.82162, 4.91752, 5.048, 4.95622, 4.81891]
    # partitioned_time = [3.246511, 3.76592, 6.295967, 13.134908, 26.531106, 45.743774, 65.257307]
    pim_tree = list(map(lambda x:100.0/x, pim_tree_time))
    partitioned = list(map(lambda x:100.0/x, partitioned_time))
    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(len(comm_labels))
    x1 = x - 0.5 * width
    x2 = x + 0.5 * width

    rects1 = ax.bar(x1, pim_tree, width, label="PIM-Tree", color='khaki', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, partitioned, width, label="Partitioned Skip List", color='cornflowerblue', edgecolor='black', zorder=3)
    ax.set_ylabel('Performance (Mop/s)', fontsize=40)
    ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels_, fontsize=25)
    plt.yticks(fontsize=30)
    ax.legend(fontsize=30, bbox_to_anchor=(0.41, -0.18))

    prop = [i/j for i, j in zip(pim_tree, partitioned)]
    ax2 = ax.twinx()
    ax2.plot(x, prop, label="Improvements of PIM-Tree\n over Partitioned Skip List",
        color='blue', ms=10, mfc='gold', lw=3, marker='o'
    )
    ax2.set_ylabel("Performance Improvement", fontsize=30)
    ax2.semilogy()
    plt.yticks(fontsize=30)
    ax2.legend(fontsize=30, bbox_to_anchor=(1.05, -0.19))
    for aa,bb in zip(x, prop):
        plt.text(aa-0.1, bb*1.1, '%.2f' % bb, ha='center', va= 'bottom',
            weight='demibold', fontsize=30
        )
    fig.tight_layout()
    plt.savefig('scan.pdf')
    plt.show()


def throughput_with_traditional_indexes():
    # performance traditional
    width = 0.15

    comm_labels = ["", pred_str + alpha_0_str, "", pred_str + alpha_1_str, "", ins_str + alpha_0_str, "", ins_str + alpha_1_str]
    type_count = 4

    pim_tree = [find_result("pim_tree", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # pim_tree = [7.01398, 8.01301, 21.7865, 22.1934]
    pim_tree = divlist(pim_tree, 1e6)
    #pim_tree = [2.63848, 2.83664, 2.78207, 2.77227]
    ab_tree = [find_result("pim_tree", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # ab_tree = divlist(ab_tree, 1e6)
    # ab_tree = [18.5271, 20.6315, 2.66731, 2.81355]
    bst = [find_result("pim_tree", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # bst = divlist(bst, 1e6)
    # bst = [6.2759, 7.13796, 2.74216, 2.8923]
    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(type_count)
    x1 = x - 1 * width
    x2 = x
    x3 = x + 1 * width

    rects1 = ax.bar(x1, pim_tree, width, label="PIM-Tree", color='khaki', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, ab_tree, width, label="Brown (a,b)-Tree", color='lightcoral', edgecolor='black', zorder=3)
    rects3 = ax.bar(x3, bst, width, label="Bronson BST", color='cornflowerblue', edgecolor='black', zorder=3)

    ax.set_ylabel('Performance (Mop/s)', fontsize=40)
    #ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels, fontsize=25)
    plt.yticks(fontsize=30)
    ax.legend(bbox_to_anchor=(0.5,0.78))
    ax.legend(prop={"size": 19, 'weight':'bold'})

    plt.yticks(fontsize=30)
    fig.tight_layout()
    plt.savefig('traditional_throughput.pdf')
    plt.show()

def component_time():
    # Time components
    width = 0.35
    comp_labels = [
        "Search\nPartitioned\n" + r"$\alpha=0$", "Search\nPartitioned\n" + r"$\alpha=0.6$",
        "Insert\nPartitioned\n" + r"$\alpha=0$", "Insert\nPartitioned\n" + r"$\alpha=0.6$",
        "Search\nPIM-tree\n" + r"$\alpha=0$", "Search\nPIM-tree\n" + r"$\alpha=0.6$",
        "Insert\nPIM-tree\n" + r"$\alpha=0$", "Insert\nPIM-tree\n" + r"$\alpha=0.6$",
    ]
    # y_labels = ["0", "0.1", "0.2", "0.3", "0.4", "0.5"]
    
    pim_exec = [find_result(idt, s, opt, 1000000, "time_pim") for idt in ("range_partitioning", "pim_tree") for opt in ("micro_predecessor", "micro_insert") for s in (0, 0.6)]
    # pim_exec = [0.009899, 0.284721, 0.014361, 0.454834, 0.02158, 0.01517237, 0.039296551, 0.032232897]

    comm = [find_result(idt, s, opt, 1000000, "time_comm") for idt in ("range_partitioning", "pim_tree") for opt in ("micro_predecessor", "micro_insert") for s in (0, 0.6)]
    # comm = [0.006505, 0.115589, 0.004636, 0.082014, 0.02311, 0.033272158, 0.05725564, 0.066251297]

    func_ld = [find_result(idt, s, opt, 1000000, "time_load") for idt in ("range_partitioning", "pim_tree") for opt in ("micro_predecessor", "micro_insert") for s in (0, 0.6)]
    # func_ld = [0, 0, 0, 0, 0, 0, 0.0552144, 0.0601772]

    cpu_exec = [find_result(idt, s, opt, 1000000, "time_cpu") for idt in ("range_partitioning", "pim_tree") for opt in ("micro_predecessor", "micro_insert") for s in (0, 0.6)]
    # cpu_exec = [0.01285945, 0.01263035, 0.01844316, 0.01487307, 0.04648, 0.054067472, 0.066098409, 0.071179607]

    b1 = list_sum(pim_exec, comm)
    b2 = list_sum(b1, func_ld)

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    plt.yticks(fontsize=30)

    ax.bar(comp_labels, pim_exec, width, label='PIM Execution', edgecolor='black', zorder=3, color="khaki")
    ax.bar(comp_labels, comm, width, bottom=pim_exec, label='CPU-PIM Communication', edgecolor='black', zorder=3, color="cornflowerblue")
    ax.bar(comp_labels, func_ld, width, bottom=b1, label='PIM Function Loading', edgecolor='black', zorder=3, color="palegreen")
    ax.bar(comp_labels, cpu_exec, width, bottom=b2, label='CPU Exectuion', edgecolor='black', zorder=3, color="lightcoral")
    plt.gca().set_xticklabels(comp_labels, fontsize=15)

    ax.set_ylabel('Time / sec', fontsize=30)
    # ax.set_title('Time breakdown on each component', fontsize=25)
    ax.legend(prop={"size": 19, 'weight':'bold'})

    plt.savefig('time_breakdown.pdf')
    plt.show()

def effect_of_optimizations():
    # Optimizations
    width = 0.15
    comm_labels_ = ["", "", "Predecessor\n" + r'$\alpha=0$', "", "Predecessor\n" + r'$\alpha=1$', "", "Insert\n" + r'$\alpha=0$', "", "Insert\n" + r'$\alpha=1$']
    comm_labels = ["Predecessor\n" + r'$\alpha=0$', "Predecessor\n" + r'$\alpha=1$', "Insert\n" + r'$\alpha=0$', "Insert\n" + r'$\alpha=1$']
    jump_push = [find_result("jump_push", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # jump_push = [222.294794, 224.528700, 255.410889, 260.711387]

    push_pull = [find_result("push_pull", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # push_pull = [67.495015, 46.855559, 87.698647, 75.256654]

    push_pull_chunk = [find_result("push_pull_chunk", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # push_pull_chunk = [11.5339, 10.6762, 22.3526, 21.0983]
    
    push_pull_chunk_shadow = [find_result("push_pull_chunk_shadow", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # push_pull_chunk_shadow = [9.11693, 10.0665, 21.7865, 22.1934]
    
    pim_tree = [find_result("pim_tree", s, opt, 1000000, "throughput") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # pim_tree = [7.01398, 8.01301, 21.7865, 22.1934]

    # jump_push = [100.0/l for l in jump_push]
    # push_pull = [100.0/l for l in push_pull]
    # push_pull_chunk = [100.0/l for l in push_pull_chunk]
    # push_pull_chunk_shadow = [100.0/l for l in push_pull_chunk_shadow]
    # pim_tree = [100.0/l for l in pim_tree]

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    #plt.semilogy()
    plt.yticks(fontsize=30)

    x = np.arange(len(comm_labels))
    x1 = x - 2 * width
    x2 = x - 1 * width
    x3 = x + 0 * width
    x4 = x + 1 * width
    x5 = x + 2 * width

    rects1 = ax.bar(x1, jump_push, width, label=r'Jump-Push based', edgecolor='black', zorder=3, color="khaki")
    rects2 = ax.bar(x2, push_pull, width, label=r'Push-Pull based', edgecolor='black', zorder=3, color="cornflowerblue")
    rects3 = ax.bar(x3, push_pull_chunk, width, label=r'Push-Pull + Chunk', edgecolor='black', zorder=3, color="palegreen")
    rects4 = ax.bar(x4, push_pull_chunk_shadow, width, label=r'Push-Pull + Chunk + Shadow', edgecolor='black', zorder=3, color="lightcoral")
    rects5 = ax.bar(x5, pim_tree, width, label=r'PIM-tree', edgecolor='black', zorder=3, color="violet")

    ax.set_ylabel('Performance (Mop/s)', fontsize=30)
    #ax.set_title('Impact of Optimizations', fontsize=30)
    plt.gca().set_xticklabels(comm_labels_, fontsize=30)
    ax.legend(fontsize=18, bbox_to_anchor=(0.5,0.78))
    fig.tight_layout()

    plt.savefig('optimization.pdf')
    plt.show()

def communication():
    # Communication
    width = 0.15
    comm_labels = ["", pred_str + alpha_0_str, "", pred_str + alpha_1_str, "", ins_str + alpha_0_str, "", ins_str + alpha_1_str]
    type_count = 4

    # y_labels = ["0", "0.1", "0.2", "0.3", "0.4", "0.5"]
    pim_pim = [find_result("pim_tree", s, opt, 1000000, "comm_pim") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # pim_pim = [102.0177888, 129.284592, 330.714192, 355.8748992]
    pim_dram = [find_result("pim_tree", s, opt, 1000000, "comm_dram") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # pim_dram = [7.25876, 7.95643, 15.4922, 15.4922]
    # pim_dram = [i * 64 for i in pim_dram]
    # pim_dram = list_sum(pim_pim, pim_dram)

    range_pim = [find_result("range_partitioning", s, opt, 1000000, "comm_pim") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # range_pim = [2798206976 / 1e8, 441322717184 / 1e8, 2824617984 / 1e8, 0]
    range_dram = [find_result("range_partitioning", s, opt, 1000000, "comm_dram") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # range_dram = cache_line_to_bytes([0.61653026, 123.1151183, 1.17339381, 0])
    range_dram = list_sum(range_pim, range_dram)
    range_dram = [min(i, 4000.0) for i in range_dram]
    # range_dram[1] = 0

    ab_dram = [find_result("ab-tree", s, opt, 1000000, "comm_dram") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # ab_dram = [17.9261, 14.8452, 56.9125, 52.3052]
    # ab_dram = [i * 64 for i in ab_dram]
    
    bst_dram = [find_result("bst", s, opt, 1000000, "comm_dram") for opt in ("micro_predecessor", "micro_insert") for s in (0.0, 1.0)]
    # bst_dram = [36.857, 30.7022, 43.7481, 39.8015]
    # bst_dram = [i * 64 for i in bst_dram]

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(type_count)
    x1 = x - 1.5 * width
    x2 = x - 0.5 * width
    x3 = x + 0.5 * width
    x4 = x + 1.5 * width

    rects1 = ax.bar(x1, pim_dram, width, label="PIM-Tree: CPU-DRAM", color='cornflowerblue', edgecolor='black', zorder=3)
    rects2 = ax.bar(x1, pim_pim, width, label="PIM-Tree: CPU-PIM", color='khaki', edgecolor='black', zorder=4)
    rects3 = ax.bar(x2, range_dram, width, label="range-partitioning: CPU-DRAM", color='violet', edgecolor='black', zorder=3)
    rects4 = ax.bar(x2, range_pim, width, label="range-partitioning: CPU-PIM", color='gray', edgecolor='black', zorder=4)
    rects5 = ax.bar(x3, ab_dram, width, label="Brown (a,b)-Tree: CPU-DRAM", color='lightcoral', edgecolor='black', zorder=3)
    rects6 = ax.bar(x4, bst_dram, width, label="Bronson BST: CPU-DRAM", color='palegreen', edgecolor='black', zorder=3)
    point = ax.scatter([x2[3]], 200, point_size, color='red', zorder=4, marker='X')
    plt.text(x2[1], 200, ">1e4", ha='center', va= 'bottom',
            weight='normal', fontsize=20)

    ax.set_ylabel('Bytes Transmitted per Element', fontsize=30)
    ax.set_ylim([0, 4000])
    #ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels, fontsize=25)
    plt.yticks(fontsize=30)

    ax.legend(bbox_to_anchor=(0.5,0.68))
    ax.legend(prop={"size": 19, 'weight':'bold'})
    #ax.legend(prop={"size": 19, 'weight':'bold'})

    plt.yticks(fontsize=30)
    fig.tight_layout()
    plt.savefig('traditional_communication.pdf')
    plt.show()

def communication_over_different_batch_size():
    # Get with different batch size
    width = 0.35
    #comm_labels = ["", "20K", "50K", "100K", "200K", "500K", "1M"]
    comm_labels = ["", "1M", "500K", "200K", "100K", "50K", "20K"]
    type_count = 6

    num_communication = [find_result("pim_tree", "micro_predecessor", 0.0, bs, "comm_pim") for bs in (1000000, 500000, 200000, 100000, 50000, 20000)]
    # num_communication = [2.20916, 1.99844, 2.5946, 3.79942, 5.77012, 7.25876]
    # num_communication.reverse()
    # num_communication = [i * 64 for i in num_communication]

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(type_count)

    rects = ax.bar(x, num_communication, width, label="PIM-Tree: CPU-DRAM", color='cornflowerblue', edgecolor='black', zorder=3)

    ax.set_ylabel('Bytes Transmitted per Operation', fontsize=30)
    #ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels, fontsize=25)
    plt.yticks(fontsize=30)
    ax.legend(bbox_to_anchor=(0.4,0.68))
    ax.legend(prop={"size": 19, 'weight':'bold'})

    #prop = [i/j for i, j in zip(pim_tree, partitioned)]
    #ax2 = ax.twinx()
    #ax2.plot(x, prop, label="Improvements of PIM-Tree\n over Partitioned Skip List",
    #    color='blue', ms=10, mfc='gold', lw=3, marker='o'
    #)
    #ax2.set_ylabel("Performance Improvement", fontsize=30)
    #ax2.semilogy()
    #plt.yticks(fontsize=30)
    #ax2.legend(fontsize=30, bbox_to_anchor=(1.05, -0.19))
    #for aa,bb in zip(x, prop):
    #    plt.text(aa-0.1, bb*1.1, '%.2f' % bb, ha='center', va= 'bottom',
    #        weight='demibold', fontsize=35
    #    )
    fig.tight_layout()
    plt.savefig('pim_tree_different_batch_size.pdf')
    plt.show()

def energy():
    # Energy
    # Scan
    width = 0.35
    comp_labels = [
        "PIM-Tree\n" + r"$\alpha=0$",
        "PIM-Tree\n" + r"$\alpha=0.6$",
        "PIM-Tree\n" + r"$\alpha=0.99$",
        "Partitioned\n" + r"$\alpha=0$",
        "Partitioned\n" + r"$\alpha=0.6$",
        "Partitioned\n" + r"$\alpha=0.99$",
    ]
    y_labels = ["0", "2000", "4000", "6000", "8000", "10000", "12000"]
    cpu_e = [717.08, 633.36, 538, 640.03, 2482.79, 8819.99]
    comm = [226.28, 171.66, 194.09, 192.36, 721.07, 2540.11]
    dpu_e = [538.7525529, 524.9919018, 504.4343074, 255.5341003, 246.3803621, 240.4565397]

    b1 = list_sum(cpu_e, comm)

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)

    ax.bar(comp_labels, cpu_e, width, label='CPU Core', edgecolor='black', zorder=3, color="khaki")
    ax.bar(comp_labels, comm, width, bottom=cpu_e, label='CPU-RAM', edgecolor='black', zorder=3, color="cornflowerblue")
    ax.bar(comp_labels, dpu_e, width, bottom=b1, label='DPU', edgecolor='black', zorder=3, color="lightcoral")
    plt.gca().set_xticklabels(comp_labels, fontsize=20)
    plt.gca().set_yticklabels(y_labels, fontsize=25)

    ax.set_ylabel('Energy / J', fontsize=30)
    # ax.set_title('Energy Cost of 1M Scan Operations with size 100\n', fontsize=30)
    ax.legend(fontsize=25)

    plt.show()

    #Search
    width = 0.35
    comp_labels = [
        "PIM-Tree\n" + r"$\alpha=0$",
        "PIM-Tree\n" + r"$\alpha=0.6$",
        "PIM-Tree\n" + r"$\alpha=0.99$",
        "Partitioned\n" + r"$\alpha=0$",
        "Partitioned\n" + r"$\alpha=0.6$",
        "Partitioned\n" + r"$\alpha=0.99$",
    ]
    y_labels = ["0", "5000", "10000", "15000", "20000", "25000", "30000", "35000", "40000"]
    cpu_e = [1004.72, 1216.78, 2164.59, 565.35,10392.22,27984.85]
    comm = [336.06,410.59,1138.38, 175.96,2914.23,8288.11]
    dpu_e = [4868.391501,4171.9532,3719.011423, 2902.79902,3081.659898,3169.39213]

    b1 = list_sum(cpu_e, comm)

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)

    ax.bar(comp_labels, cpu_e, width, label='CPU Core', edgecolor='black', zorder=3, color="khaki")
    ax.bar(comp_labels, comm, width, bottom=cpu_e, label='CPU-RAM', edgecolor='black', zorder=3, color="cornflowerblue")
    ax.bar(comp_labels, dpu_e, width, bottom=b1, label='DPU', edgecolor='black', zorder=3, color="lightcoral")
    plt.gca().set_xticklabels(comp_labels, fontsize=20)
    plt.gca().set_yticklabels(y_labels, fontsize=25)

    ax.set_ylabel('Energy / J', fontsize=30)
    # ax.set_title('Energy Cost of 100M Search Operations\n', fontsize=30)
    ax.legend(fontsize=25)

    # plt.show()
    plt.show()

    # Insert
    width = 0.35
    comp_labels = [
        "PIM-Tree\n" + r"$\alpha=0$",
        "PIM-Tree\n" + r"$\alpha=0.6$",
        "PIM-Tree\n" + r"$\alpha=0.99$",
        "Partitioned\n" + r"$\alpha=0$",
        "Partitioned\n" + r"$\alpha=0.6$",
        "Partitioned\n" + r"$\alpha=0.99$",
    ]
    y_labels = ["0", "10000", "20000", "30000", "40000", "50000", "60000"]
    cpu_e = [3728.47,4101.35,3741.94, 530.24,7020.36,39444.44]
    comm = [1130.66,1296.75,1160.92, 176.51,2144.02,11788.38]
    dpu_e = [7701.138451,7306.843123,6485.494844, 4613.918288,4390.460219,4267.308624]

    b1 = list_sum(cpu_e, comm)

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)

    ax.bar(comp_labels, cpu_e, width, label='CPU Core', edgecolor='black', zorder=3, color="khaki")
    ax.bar(comp_labels, comm, width, bottom=cpu_e, label='CPU-RAM', edgecolor='black', zorder=3, color="cornflowerblue")
    ax.bar(comp_labels, dpu_e, width, bottom=b1, label='DPU', edgecolor='black', zorder=3, color="lightcoral")
    plt.gca().set_xticklabels(comp_labels, fontsize=20)
    plt.gca().set_yticklabels(y_labels, fontsize=25)

    ax.set_ylabel('Energy / J', fontsize=30)
    # ax.set_title('Energy Cost of 100M Insert Operations\n', fontsize=30)
    ax.legend(fontsize=25)

    plt.show()

def ycsb():
    # YCSB
    width = 0.15
    #comm_labels_ = ["", "PIM-tree", "", "Partitioned skip list"]
    #comm_labels = ["PIM-tree", "Partitioned skip list"]
    comm_labels_ = ["", "", "Partitioned\n" + r'$\alpha=0$', "", "Partitioned\n" + r'$\alpha=1$', "", "PIM-tree\n" + r'$\alpha=0$', "", "PIM-tree\n" + r'$\alpha=1$']
    comm_labels = ["Partitioned\n" + r'$\alpha=0$', "Partitioned\n" + r'$\alpha=1$', "PIM-tree\n" + r'$\alpha=0$', "PIM-tree\n" + r'$\alpha=1$']

    workload_A = [find_result(idt, s, "ycsb_a", 1000000, "throughput") for idt in ("range_partitioning", "pim_tree") for s in (0, 1.0)]
    # workload_A = [3.482651, 117.316418, 16.4503, 17.2022]
    workload_B = [find_result(idt, s, "ycsb_b", 1000000, "throughput") for idt in ("range_partitioning", "pim_tree") for s in (0, 1.0)]
    # workload_B = [2.980891, 118.219794, 10.1112, 10.4659]
    workload_C = [find_result(idt, s, "ycsb_c", 1000000, "throughput") for idt in ("range_partitioning", "pim_tree") for s in (0, 1.0)]
    # workload_C = [2.926345, 227.026608, 7.01398, 8.01301]
    workload_D = [find_result(idt, s, "ycsb_d", 1000000, "throughput") for idt in ("range_partitioning", "pim_tree") for s in (0, 1.0)]
    # workload_D = [3.744016, 10000, 21.7865, 22.1934]
    workload_E = [find_result(idt, s, "ycsb_e", 1000000, "throughput") for idt in ("range_partitioning", "pim_tree") for s in (0, 1.0)]
    # workload_E = [5, 5, 5, 5]

    # workload_A = [100.0/l for l in workload_A]
    # workload_B = [100.0/l for l in workload_B]
    # workload_C = [100.0/l for l in workload_C]
    # workload_D = [100.0/l for l in workload_D]
    # workload_E = [100.0/l for l in workload_E]

    fig, ax = plt.subplots(figsize=(15, 9))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    #plt.semilogy()
    plt.yticks(fontsize=30)

    x = np.arange(len(comm_labels))
    x1 = x - 2 * width
    x2 = x - 1 * width
    x3 = x + 0 * width
    x4 = x + 1 * width
    x5 = x + 2 * width

    rects1 = ax.bar(x1, workload_A, width, label=r'YCSB Workload A', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, workload_B, width, label=r'YCSB Workload B', edgecolor='black', zorder=3)
    rects3 = ax.bar(x3, workload_C, width, label=r'YCSB Workload C', edgecolor='black', zorder=3)
    rects4 = ax.bar(x4, workload_D, width, label=r'YCSB Workload D', edgecolor='black', zorder=3)
    rects5 = ax.bar(x5, workload_E, width, label=r'YCSB Workload E', edgecolor='black', zorder=3)

    ax.set_ylabel('Performance (Melement/s)', fontsize=30)
    ax.set_title('Impact of Optimizations', fontsize=30)
    plt.gca().set_xticklabels(comm_labels_, fontsize=30)
    ax.legend(fontsize=23, bbox_to_anchor=(0.65,0.62))
    fig.tight_layout()

    plt.savefig('YCSB.pdf')
    plt.show()

#quit()

def wikipedia():
    # wikipedia result
    width = 0.15

    comm_labels = ["", "", pred_str, "", "", "", "", ins_str]
    type_count = 2

    # y_labels = ["0", "0.1", "0.2", "0.3", "0.4", "0.5"]
    pim_tree_throughput = [find_result("pim_tree", 0.0, ops, 2000000, "throughput") for ops in ("wiki_predecessor", "wiki_insert")]
    # pim_tree_throughput = [22.636, 9.9333]
    ab_tree_throughput = [find_result("ab-tree", 0.0, ops, 2000000, "throughput") for ops in ("wiki_predecessor", "wiki_insert")]
    # ab_tree_throughput = [27.8993, 8.65881]
    bst_tree_throughput = [find_result("bst", 0.0, ops, 2000000, "throughput") for ops in ("wiki_predecessor", "wiki_insert")]
    # bst_tree_throughput = [17.8418, 5.14087]

    pim_tree_dram_communication = [find_result("pim_tree", 0.0, ops, 2000000, "comm_dram") for ops in ("wiki_predecessor", "wiki_insert")]
    # pim_tree_dram_communication = cache_line_to_bytes([4.6427859, 8.88064752])
    pim_tree_pim_communication = [find_result("pim_tree", 0.0, ops, 2000000, "comm_pim") for ops in ("wiki_predecessor", "wiki_insert")]
    # pim_tree_pim_communication = [39.7901824, 129.63037184]
    pim_tree_communication = list_sum(pim_tree_dram_communication, pim_tree_pim_communication)

    ab_tree_communication = [find_result("ab-tree", 0.0, ops, 2000000, "comm_dram") for ops in ("wiki_predecessor", "wiki_insert")]
    # ab_tree_communication = cache_line_to_bytes([9.95039, 13.5635])
    bst_tree_communication = [find_result("bst", 0.0, ops, 2000000, "comm_dram") for ops in ("wiki_predecessor", "wiki_insert")]
    # bst_tree_communication = cache_line_to_bytes([12.76977, 31.8681])

    #pim_pim = [102.0177888, 129.284592, 330.714192, 355.8748992]
    #pim_dram = [7.25876, 7.95643, 15.4922, 15.4922]
    #pim_dram = [i * 64 for i in pim_dram]
    #pim_dram = list_sum(pim_pim, pim_dram)
    #
    #ab_dram = [15.0578, 10.7604, 51.3365, 46.4506]
    #ab_dram = [i * 64 for i in ab_dram]
    #
    #bst_dram = [31.4404, 25.1749, 39.1331, 36.4327]
    #bst_dram = [i * 64 for i in bst_dram]

    fig, ax = plt.subplots(figsize=(14, 8))
    plt.grid(axis="y", linestyle='-.', zorder=0)
    x = np.arange(type_count)
    print(x)
    x1 = x - 1 * width
    x2 = x
    x3 = x + 1 * width

    rects1 = ax.bar(x1, pim_tree_throughput, width, label="PIM-Tree", color='khaki', edgecolor='black', zorder=3)
    rects2 = ax.bar(x2, ab_tree_throughput, width, label="Brown (a,b)-Tree", color='lightcoral', edgecolor='black', zorder=3)
    rects3 = ax.bar(x3, bst_tree_throughput, width, label="Bronson BST", color='cornflowerblue', edgecolor='black', zorder=3)

    ax2 = ax.twinx()
    point1 = ax2.scatter(x1, pim_tree_communication, point_size, color='black', zorder=4, marker='P')
    point2 = ax2.scatter(x2, ab_tree_communication, point_size, color='black', zorder=4, marker='P')
    point3 = ax2.scatter(x3, bst_tree_communication, point_size, color='black', zorder=4, marker='P')
    for aa,bb in zip(x1, pim_tree_communication):
        plt.text(aa, bb + 100, '%.0f' % bb, ha='center', va= 'bottom',
            weight='normal', fontsize=25
        )
    for aa,bb in zip(x2, ab_tree_communication):
        plt.text(aa, bb + 100, '%.0f' % bb, ha='center', va= 'bottom',
            weight='normal', fontsize=25
        )
    for aa,bb in zip(x3, bst_tree_communication):
        plt.text(aa, bb + 100, '%.0f' % bb, ha='center', va= 'bottom',
            weight='normal', fontsize=25
        )
        
    ax2.set_ylim([0, 3500])
    ax.set_ylabel('Throughput (Mop/s)', fontsize=30)
    ax2.set_ylabel('Bytes Transmitted per Operation', fontsize=30)

    #ax.set_xlabel(r"Parameter $\alpha$ in Zipfian Distribution", fontsize=40)
    # ax.set_title('Performance of Get Operation', fontsize=25)
    plt.gca().set_xticklabels(comm_labels, fontsize=25)
    plt.yticks(fontsize=30)
    ax.tick_params(labelsize=30)
    ax.legend(bbox_to_anchor=(0.5,0.68))
    ax.legend(prop={"size": 25, 'weight':'bold'})
    #ax.legend(prop={"size": 19, 'weight':'bold'})

    fig.tight_layout()
    plt.savefig('wikipedia_throughput.pdf')
    plt.show()
