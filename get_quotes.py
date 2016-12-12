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

        except KeyboardInterrupt:
            raise

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

        except KeyboardInterrupt:
            raise
        
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


def returns(price_start, price_end, actions):
    """Takes price difference between two dates, and adjusts for dividends and splits to produce total return as a percent.

    :ticker: list of ticker symbols 
    :start: start date
    :end: end date
    :returns: total return as a percentage 

    """


    #should only need to multiply the start date closing price by the split value (1/10 split has value '10')
    #1. add all dividends onto end price
    #2. multiply start close price by split
    
    #TODO:
    #Yahoo finance assumes most recent split price when reporting historic dividends. 
    #This will be a problem when adding dividend info during backtesting.  
    #Might be better to either use div yield or google finance

    
    
    #join price_start with price_end on 'Ticker'
    prices = pd.merge(price_start, price_end, on='Ticker', suffixes=['_start','_end'])[['Close_start', 'Close_end', 'Ticker']]

    #get sum of dividends and product of splits
    div_totals = actions[actions.action=='DIVIDEND'].groupby('Ticker', as_index=False)['value'].sum().rename(columns = {'value':'dividends'})
    split_totals = actions[actions.action=='SPLIT'].groupby('Ticker', as_index=False)['value'].prod().rename(columns = {'value':'splits'})

    #join prices with div_totals and split_totals to get div and split cols
    prices_div_split = pd.merge(pd.merge(prices, div_totals, on='Ticker', how='left'), split_totals, on='Ticker', how='left')

    #fill NaNs with 0 for dividends, and 1 for splits (sum dividends, multiply splits)
    prices_div_split['dividends'] = prices_div_split['dividends'].fillna(0)
    prices_div_split['splits'] = prices_div_split['splits'].fillna(1)

    #adjust start and end close prices based on dividend and split info
    prices_div_split['div_adj_Close_end'] = (prices_div_split.Close_end + prices_div_split.dividends)
    prices_div_split['split_adj_Close_start'] = (prices_div_split.Close_start * prices_div_split.splits)

    prices_div_split['perc_change'] = (prices_div_split.div_adj_Close_end - prices_div_split.split_adj_Close_start)/prices_div_split.split_adj_Close_start

    change = prices_div_split.perc_change.mean()
    
    return prices_div_split, change


def main():
    """program logic"""

    #convert dates to datetime

    #create price and actions dataframes
    price_start = historic_prices(ticker, start)
    price_end = historic_prices(ticker, end)
    actions = corporate_actions(ticker, start, end)

    prices_div_split, change = returns(price_start, price_end, actions)

if __name__=='__main__':
    main
