import pandas as pd
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
    #slashes replaced with '_div_' to signify 'divided by'
    cols = colnames[1].tolist()
    col_underscore = [name.replace(' ', '_') for name in cols]
    col_slash = [name.replace('/', '_div_') for name in col_underscore]
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


def deciles(df, start_col=3, end_col=None):
    """Adds columns that rank each column by decile.

    :df: Dataframe
    :start_col: index val of first column in df for which 
        we want to add a decile ranking column.
        default=3
    :end_col: index val of last column in df for which 
        we want to add a decile ranking column.
        default=last index value of df

    :returns: df with added decile ranking columns

    """
    
    if not end_col:
        end_col=df.shape[1]+1

    for col in df.columns.tolist()[start_col:end_col]:
        colstring = 'decile_{}'.format(col)
        df[colstring] = pd.qcut(df[col], 10, labels=False)

    return df


def lowest_deciles(df, low_factor_cols):
    """Returns dataframe with lowest_decile values for 
    each specified col in df.

    :factor_cols: list of columns for which to filter out by lowest decile
    
    :returns: nothing; saves csv file of lowest decile stock data for 
    for each colname in low_factor_cols

    example:
        lowest_deciles(df, ['Price_div_CFPS', 'Enterprise_Value_div_EBITDA'])
        saves as csv the following 2 dataframes: 
            dataframe with only stocks in decile 1 of Price/CFPS
            dataframe with only stocks in decile 1 of Enterprise_Value/EBITDA 

    """

    for col in low_factor_cols:
        decile_col = 'decile_{}'.format(col)
        df_save = df[df[decile_col]==0]
        df_save.to_csv('lowest_decile_{}.csv'.format(col),index=False)


def highest_deciles(df, high_factor_cols):
    """Returns dataframe with highest_decile values for 
    each specified col in df.

    :factor_cols: list of columns for which to filter out by highest decile
    
    :returns: nothing; saves csv file of highest decile stock data for 
    for each colname in high_factor_cols

    example:
        highest_deciles(df, ['Price_change_26_week'])
        saves as csv the following dataframe: 
            dataframe with only stocks in decile 10 of Price_change_26_week

    """

    for col in high_factor_cols:
        decile_col = 'decile_{}'.format(col)
        df_save = df[df[decile_col]==9]
        df_save.to_csv('highest_decile_{}.csv'.format(col),index=False)


def initialize_parser():
    """Initialize argparser
    """

    p=ArgumentParser(
            description='This program is designed to: \n\t'
            '--take in a filename, key_file, and up to two \n\t'
                'optional lists of factors\n\t'
            '--for each factor in the list(s) of factors:\n\t'
                '*a csv file of the lowest (highest) decile stocks will be saved\n\t'
                '*the file(s) will be saved in the current directory\n\t'
                '*factor names must match column names in column two of key_file\n\t',
            formatter_class=RawTextHelpFormatter
    )
    p.add_argument('filename', type=str, help='filename for data ie ' 
                    'TRENDINGVALUE.TXT')
    p.add_argument('key_file', type=str, help='column name file for '
                    'data ie TRENDINGVALUE_Key.TXT')
    
    #optional args:
    p.add_argument('--low_factor_cols', type=str, help= 'comma-separated '
                    'string of columns from which to return lowest '
                    'decile value stock data as csv files')
    p.add_argument('--high_factor_cols', type=str, help= 'comma-separated '
                    'string of columns from which to return highest '
                    'decile value stock data as csv files')
    return p
                

def main():
    """program logic"""
   
    #initialize arg_parser and parse arguments
    p=initialize_parser()
    args = p.parse_args()

    filename = args.filename
    key_file = args.key_file
    low_factor_cols = args.low_factor_cols.strip().split(',') if args.low_factor_cols else 'None'
    high_factor_cols = args.high_factor_cols.strip().split(',') if args.high_factor_cols else 'None'


    #get data
    df = read_data(filename, key_file)
    
    #replace fill_val with null vals
    df = insert_nulls(df)
   
    #filter out low market cap stocks
    df = market_cap(df)
        
    #add decile ranking columns and save df as csv
    df = deciles(df)
    df.to_csv('aaii_allstocks.csv',index=False)

    #save lowest decile values as csv
    if low_factor_cols != 'None':
        lowest_deciles(df, low_factor_cols)

    #save highest decile values as csv
    if high_factor_cols != 'None':
        highest_deciles(df, high_factor_cols)
    

#run program logic:
if __name__=='__main__':
    main()
