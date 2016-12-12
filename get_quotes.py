import pandas_datareader.data as dr
import datetime
import pandas as pd

def historic_prices(ticker, date):
    """Takes in a list of ticker symbols and date, and returns dataframe of
    closing prices on date for all symbols in ticker
    """
   
    price_data=pd.DataFrame()

    for t in ticker:
        
        try:
            data_append = pd.DataFrame(dr.DataReader(t, 'yahoo', date, date))
            data_append['Ticker'] = t
            price_data = price_data.append(data_append)

        except: 
            print('cannot find data for stock {ticker} on date {date}'.format(ticker=t, date=date.strftime("%B %d, %Y")))
            continue

    return price_data


def corporate_actions(ticker, start, end):
    """Takes in a list of ticker symbols as well as a start and end date and 
    returns dataframe of corporate actions (dividend and stock splits)
    """

    action_data = pd.DataFrame()

    for t in ticker:
        try:
            data_append = pd.DataFrame(dr.DataReader(t, 'yahoo-actions', start, end))
            data_append['Ticker'] = t
            action_data = action_data.append(data_append)

        except:
            print('cannot find data for stock {ticker} from dates {start} to {end}'.format(ticker=t, start=start.strftime("%B %d, %Y"), end=end.strftime("%B %d, %Y")))
            continue

    return action_data


#def get_div_split(action_data):
#    """takes in corporate action dataframe and returns .
#
#    :action_data: TODO
#    :returns: TODO
#
#    """
#    pass


def returns(ticker, start, end):
    """Takes price difference between two dates, and adjusts for dividends and splits to produce total return as a percent.

    :ticker: list of ticker symbols 
    :start: start date
    :end: end date
    :returns: total return as a percentage 

    """
    #TODO: Need to confirm, but it seems as though the historic dividends data represents the dividend assuming post-split price.
    #So we should only need to multiply the start date closing price by the split value (1/10 split has value '10')
    #1. add all dividends onto end price
    #2. multiply start close price by split
    
    #create price and actions dataframes
    price_start = historic_prices(ticker, start)
    price_end = historic_prices(ticker, end)
    actions = corporate_actions(ticker, start, end)
    
    #join price_start with price_end on 'Ticker'
    prices = pd.merge(price_start, price_end, on='Ticker', suffixes=['_start','_end'])
    prices = prices[['Close_start', 'Close_end', 'Ticker']]

    #get sum of dividends
    div_totals = actions.groupby('Ticker', as_index=False)['value'].sum()
    
    #get stock split info
    splits = actions[actions.action=='SPLIT']
    
    #join prices with div_totals to get div col
    prices_div = pd.merge(prices, div_totals, on='Ticker', how='left')
    prices_div.rename(columns = {'value':'dividends'}, inplace=True)
    prices_div['dividends'] = prices_div['dividends'].fillna(0)

    prices_div['div_adj_Close_end'] = prices_div.Close_end+prices_div.dividends
    #join prices with splits to get split col
    #totals = prices_div.merge(splits, on='Ticker', how='left')
   
    prices_div_split = pd.merge(prices_div, splits, on='Ticker', how='left')
    prices_div_split.rename(columns = {'value':'split'}, inplace=True)
    prices_div_split['split'] = prices_div['split'].fillna(1)

    prices_div_split['split_adj_Close_start'] = prices_div_split.Close_start*prices_div_split.split

    prices_div_split['perc_change'] = (prices_div_split.div_adj_Close_end - prices_div_split.split_adj_Close_start)/prices_div.split_adj_Close_start

    change = prices_div_split.perc_change.mean()
    
    return prices_div_split, change


def main():
    """program logic"""

    #convert dates to datetime
