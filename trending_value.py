import pandas as pd
import numpy as np
import sys
from argparse import ArgumentParser, RawTextHelpFormatter


def read_data(filename, key_file):
    """Reads in filename and assigns column names based on 
    values in key_file, replacing spaces with '-' in column names.  
    Returns dataframe.

    :filename: aaii data file (ie TRENDING_VALUE.TXT)
    :key_file: aaii file that tells us the column names (ie 
        TRENDING_VALUE_Key.TXT
    :returns: dataframe with data from filename and columns from key_file

    """
    df = pd.read_csv(filename, header=None)
    colnames = pd.read_csv(key_file, header=None)

    #change column names to be more dot operator friendly:
    #spaces replaced with underscores
    #slashes replaced with '_to_' (ie Price/CFPS becomes Price_to_CFPS)
    cols = colnames[1].tolist()
    col_underscore = [name.replace(' ', '_') for name in cols]
    col_slash = [name.replace('/', '_to_') for name in col_underscore]
    df.columns = col_slash
    
    return df


def insert_nulls(df, fill_val=-99999999.99):
    """replaces fill_val with null in all columns of df.

    :df: Dataframe
    :fill_val: fill value to be replaced with nulls. 
        default=-99999999.99
    :returns: Dataframe with fill_val replaced with nulls

    """

    for col in df.columns.tolist():
        df.ix[df[col] == fill_val, col] = None

    return df


def market_cap(df, min_cap=200):
    """filters out stocks that don't meet the min_cap minimum capitalization
    criteria.

    :df: dataframe
    :min_cap: minimum market_cap number (*1 million) for stocks to include in
        stock universe (default = 200)
    :returns: Dataframe with stocks that have market capitalization>min_cap

    """

    df_allstocks = df[df.Market_Cap_Q1>200]

    return df_allstocks


def ranks(df, col, asc):
    """Adds columns that rank each column from 0-99 based on rank.

    :df: Dataframe
    :col: column to rank. Added column will be called 'score_<col>'
    :asc: boolean value to pass the ascending parameter in pd.rank() function

    :returns: df with added 0-99 ranking columns

    """

    try:
    	df_rank = df[col].rank(pct=True, ascending=asc)
        return pd.qcut(df_rank, 100, labels=False).fillna(49)
    
    except ValueError:
        try:
    	    df_rank = df[col].rank(pct=True, ascending=asc, method='first')
            return pd.qcut(df_rank, 100, labels=False).fillna(49)
	
	except ValueError:
	    raise(e)
            print('{} has non-unique bin edges and will not be included in ranking'.format(col))


def add_rank_cols(df):
    """loops through columns in df and adds ranking columns based on James O'Shaughnessy's
    value factor methods.  Each of the following is ranked:
        
        Shareholder_Yield
        Price to Book
        Price to Earnings
        Price to Sales
        Price to Cash Flow
        Enterprise value to EBITDA
    
    The stocks are ranked on a scale of 0-99, with the lowest one percent of Price to Earnings values 
    recieving a 99 and the highest one percent of price to earnings values recieving a 0.  The other 
    factors are ranked in the same way except for Shareholder Yield, which is assigned a 0 for the 
    lowest one percent of values and a 99 for the highest one percent.  Null values recieve a score 
    of 49.  This is consistent with James O'Shaughnessy's method exept it uses python's 0-based indexing,
    which should not have an effect on the overall scoring (O'Shaughnessy uses a 1-100 range of scoring)

        
        """

    for col in df.columns.tolist():
        colstring = 'score_{}'.format(col)

        if df[col].dtype in ('float64', 'int64'):

            if col in ('Shareholder_Yield'):
                print('assigning rank to {}'.format(col))
                df[colstring] = pd.DataFrame(ranks(df, col, True))
		continue
            
            elif col in ('Price_to_Book', 'PE', 'Price_to_Sales', 'Enterprise_Value_to_EBITDA', 
                    'Price_to_CFPS'):
                print('assigning rank to {}'.format(col))
                df[colstring] = pd.DataFrame(ranks(df, col, False))
            	continue

        else:
            print('{} will not be ranked due to datatype'.format(col))


def trending_value(df, num_stocks):
    """Appends column to df with decile rankings based on James O'Shaughnessy's value factor two
    method.  Decile 9 contains the stocks with the highest scores from the following 6 factors:
        
        Shareholder_Yield
        Price to Book
        Price to Earnings
        Price to Sales
        Price to Cash Flow
        Enterprise value to EBITDA
    
    See add_rank_cols docstring for details on how each column is scored
    
    Also appends a boolean column with True for stocks in the trending value 25 or 50 stock portfolio

    :df: Dataframe with ranked value columns (ranked 0-99)
    :num_stocks: number of value factor 2 stocks to return that have the highest 26 week price change
        (Typically either 25 or 50; default=25)

    :returns: Dataframe with the above described two columns appended as well as a sum of scores column
	***HIGHEST SCORE STOCKS ARE IN DECILE 9***
	      A second dataframe with only the num_stocks trending value stocks 

    """
   
    #add a sum of scores column
    score_cols = ['score_Price_to_Book',
	     'score_PE',
	     'score_Price_to_Sales',
	     'score_Enterprise_Value_to_EBITDA',
	     'score_Price_to_CFPS',
	     'score_Shareholder_Yield'] 
    df['sum_scores'] = df[score_cols].sum(axis=1) 

    #calculate deciles of sum_scores and create value_factor_two colum (boolean) of highest decile 
    df['decile_value_factor_two'] = pd.qcut(df['sum_scores'], 10, labels=False)
    df['value_factor_two'] = df['decile_value_factor_two']==9
     
    #find the num_stocks value factor two stocks that have the highest 6 month price change
    trending_value = df[df['value_factor_two']==True].sort('Price_Change_26_week', ascending=False)[:num_stocks]
    tickers = [ticker for ticker in trending_value['Ticker']]
    df['trending_value'] = df['Ticker'].isin(tickers)

    return trending_value
    

def initialize_parser():
    """Initialize argparser
    """

    p=ArgumentParser(
            description='This program is designed to mirror '
		'James O\'Shaughnessy\'s trending value portfolio '
		'strategy and return the following: \n\t'
            '--Take in a filename and key_file from aaii, and optional \n\t'
	        'num_stocks and raw_data args.\n\t'
	    '--A csv file containing data from the trending \n\t'
	        'value stocks will be saved in the current dir.\n\t'
	    '--A csv with raw data from the all_stocks universe \n\t'
	        'will be saved if --raw_data is passed.',
            formatter_class=RawTextHelpFormatter
    )
    p.add_argument('filename', type=str, help='filename for data ie ' 
                    'TRENDINGVALUE.TXT')
    p.add_argument('key_file', type=str, help='column name file for '
                    'data ie TRENDINGVALUE_Key.TXT')

    #optional args
    p.add_argument('--num_stocks', type=int, help='number of stocks to '
		   'include in trending value portfolio. Typically 25 or '
		   '50. Default = 25')
    p.add_argument('--raw_data', help='call flag if you want to save the '
		   'raw data as a csv', action="store_true")
    
    return p
                

def main():
    """program logic"""
   
    #initialize arg_parser and parse arguments
    p=initialize_parser()
    args = p.parse_args()

    #positional args
    filename = args.filename
    key_file = args.key_file

    #optional args
    num_stocks = args.num_stocks if args.num_stocks else 25
    raw_data = args.raw_data
    print(raw_data)

    #get data
    df = read_data(filename, key_file)
    
    #replace fill_val with null vals
    df = insert_nulls(df)
   
    #filter out low market cap stocks
    df = market_cap(df)
        
    #add decile ranking columns 
    add_rank_cols(df) 

    #add value factor 2 and trending value columns and return dataframe of trending value portfolio stocks
    trending_value_df = trending_value(df, num_stocks)

    #reset indices
    trending_value_df.reset_index()
    df.reset_index()

    #save trending value portfolio stocks as csv
    trending_value_df.to_csv('trending_value_{}_stocks.csv'.format(str(num_stocks)), index=False)
    
    #save raw data if flagged
    if raw_data:
	df.to_csv('aaii_allstocks_composite_factor.csv', index=False)  


#run program logic:
if __name__=='__main__':
    main()
