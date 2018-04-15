import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import feather

espn_brackets = feather.read_dataframe('espn_brackets_2018.feather')
espn_brackets = espn_brackets[espn_brackets['p']!=0]

my_brackets = feather.read_dataframe('scores_2018.feather')

# Full histogram
plt.clf()
sns.distplot(my_brackets.Score)
sns.distplot(espn_brackets.p)
plt.xticks(range(0, 1670, 250), fontsize=8)
plt.xlim(0, 1800)
plt.legend(["Simulated", "ESPN"])
plt.xlabel('Score')
# plt.show()
plt.savefig('full_histogram.jpg')

# Zoomed in
plt.clf()
sns.distplot(my_brackets.Score)
sns.distplot(espn_brackets.p)
plt.xticks(range(0, 1700, 25), fontsize=8)
plt.legend(["Simulated", "ESPN"])
plt.xlim(1450, 1700)
plt.ylim(0, 0.00002)
plt.xlabel('Score')
# plt.show()
plt.savefig('top_histogram.jpg')


# Full CDF
plt.clf()
sns.distplot(my_brackets.Score, hist_kws={'cumulative': True}, kde_kws={'cumulative': True})
sns.distplot(espn_brackets.p, hist_kws={'cumulative': True}, kde_kws={'cumulative': True})
plt.xticks(range(0, 1670, 250), fontsize=8)
plt.ylim(0, 1)
plt.xlim(0, 1800)
plt.xlabel('Score')
# plt.show()
plt.legend(["Simulated", "ESPN"], loc=2)
plt.savefig('full_cdf.jpg')