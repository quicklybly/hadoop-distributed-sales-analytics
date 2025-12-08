# python3 -m venv venv
# source venv/bin/activate
# pip install pandas matplotlib

import pandas as pd
import matplotlib.pyplot as plt


def plot_aggregation(df, title, filename):
    plt.figure(figsize=(12, 7))

    # Группируем по split_size и combiner
    for split_size, split_group in df.groupby(['split_size', 'combiner']):
        split, combiner = split_size
        split_group = split_group.sort_values(by='reducers')

        label = f"Split {split}, Combiner {combiner}"
        plt.plot(split_group['reducers'], split_group['aggregation_time'],
                 marker='o', label=label)

    plt.title(title)
    plt.xlabel('Reducer Count')
    plt.ylabel('Aggregation Time (s)')
    plt.legend(title='Split Size + Combiner')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


if __name__ == '__main__':
    heavy_df = pd.read_csv('heavy_alone.csv')
    except_heavy_df = pd.read_csv('except_heavy.csv')

    plot_aggregation(heavy_df, 'Heavy Alone — Aggregation Time', './images/heavy_alone_aggregation.png')
    plot_aggregation(except_heavy_df, 'Except Heavy — Aggregation Time', './images/except_heavy_aggregation.png')
