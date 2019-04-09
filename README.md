# one-million-brackets

## Initial Setup
`scrape_kenpom.py` to get the Kenpom ratings  
`create_original_bracket.ipynb` to generate the base bracket

## Generate Brackets
`bracket_predictions.py` to create brackets and save to json

## Score Brackets
`bracket_score.py` to score the saved brackets and save results

## ESPN Brackets
`espn_group_search.py` to find all the ESPN Tournament Challenge groups  
`scrape_espn_brackets.py` to scrape the brackets from those groups  
`reduce_espn.py` to convert the ESPN bracket format into a dataframe for comparison

## Results
`summarize_results.ipynb` to load results and compute summary statistics