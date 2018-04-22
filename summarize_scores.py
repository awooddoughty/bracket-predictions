import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import feather

espn_brackets = feather.read_dataframe('espn_brackets_2018.feather')
espn_brackets = espn_brackets[espn_brackets['p']!=0]

my_brackets = feather.read_dataframe('scores_2018.feather')







max_scores = df_scores[df_scores['Score']==df_scores['Score'].max()]
bracket_names_top = max_scores['Name'].tolist()
save_brackets('top', bracket_names_top)

sixteen_seeds = df_scores[df_scores['16 Total']==df_scores['16 Total'].max()]
bracket_names_16 = sixteen_seeds['Name'].tolist()
save_brackets('16_seed', bracket_names_16)

first_round = df_scores[df_scores['32']==df_scores['32'].max()]
bracket_names_first = first_round['Name'].tolist()
save_brackets('first_round', bracket_names_first)

