from sys import argv

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import cm

n_logs = len(argv) - 1

formats = ["--", "--", "-", "-"]
colors = cm.get_cmap("tab10").colors
print(len(colors))
if n_logs > 0:
    fig, ax = plt.subplots(1, 1)
    for i, log in enumerate(argv[1:]):
        data = pd.read_csv(log, comment='#')
        thr = data['Throughput'].values[10:390]
        print(np.mean(thr))
        if i == 2:
            thr[thr > np.mean(thr) + 1000] = np.mean(thr)
        avg_thr1 = np.cumsum(thr) / (np.arange(thr.shape[0]) + 1)
        avg_thr = np.mean(np.reshape(avg_thr1, (-1, 5)), axis=1)
        ax.plot(avg_thr[1:], formats[i], c=colors[(i + 1) % 3], linewidth=3)
        ax.set_xlabel('Queries Aligned (x50000)')
        ax.set_ylabel('Avg. Throughput (queries / second)')
        ax.set_ylim(10000, 14000)
        #ax.set_yticks(range(0, 14001, 2000))
        #ax.set_title('Average Throughput')

    ax.legend(["No Cache", "LRU Only", "LRU + RCMP"], loc="best")
    plt.tight_layout()
    # fig.savefig("buckets_cache.eps")
    plt.show()

    # fig, ax = plt.subplots(1, 1)
    # for i, log in enumerate(argv[2:]):
    #     data = pd.read_csv(log, comment='#')
    #     hits = data['Hits']
    #     ax.plot(hits[1:], linewidth=3, c=colors[(i + 2)%3])
    #     ax.set_xlabel('Queries Aligned (x50000)')
    #     ax.set_ylabel('Cumulative Cache Hits')
    #     #ax.set_yticks(range(0, 14001, 2000))
    #     #ax.set_title('Average Throughput')
    #
    # ax.legend(["LRU Only", "LRU + SeAlM"], loc="best")
    # plt.tight_layout()
    # # fig.savefig("buckets_cache.eps")
    # plt.show()
else:
    print('No logs to plot.')
