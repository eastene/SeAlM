from sys import argv

import pandas as pd
import matplotlib.pyplot as plt

n_logs = len(argv) - 1

if n_logs > 0:
    fig, ax = plt.subplots(1, 2)
    for log in argv[1:]:
        data = pd.read_csv(log)
        ax[0].plot(data['Throughput'][:-1])
        ax[0].set_xlabel('Batches (x100000 reads)')
        ax[0].set_ylabel('Throughput (reads/second)')
        ax[0].set_title('Average Throughput')
        ax[1].plot(data['Hits'][:-1])
        ax[1].set_xlabel('Batches (x100000 reads)')
        ax[1].set_ylabel('Hits')
        ax[1].set_title('Accumulative Cache Hits')
    fig.legend(argv[1:])
    plt.show()
else:
    print('No logs to plot.')
