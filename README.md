# bracket-predictions

## Initial Setup
Update the year in constants!!
Create directories
Subscribe to Silver Bulletin and download xlsx files. Rename to `initial` and `silver`
`scrape_kenpom.ipynb` to get the Kenpom ratings  and add to simple
`create_original_brackets.ipynb` to generate the base brackets and Silver predictions

## Generate Brackets
`generate_predictions.ipynb` to create brackets and save to json

## Score Brackets
`score_brackets.ipynb` to score the saved brackets and save results  
`manual_scoring.ipynb` to count brackets with different outcomes

## ESPN Brackets
`scrape_espn.ipynb` to find and scrape all the ESPN Tournament Challenge group brackets and save  
`score_espn.ipynb` to score those brackets

## Results
`summarize_results.ipynb` to load results and compute summary statistics  
`create_probability_from_bracket` to compute probabilities of different brackets