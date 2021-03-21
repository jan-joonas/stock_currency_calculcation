import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, Column, Numeric, String, Date, MetaData, Integer, and_ 

class UserInputException(Exception):
    pass
class ApiExpception(Exception):
    pass

def load_data_stock(symbol, start_date, end_date):        
    # requesting needed data 
    data = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&outputsize=full&apikey={}'.format(symbol,api_key))

    print('Data loading json format for stock {}: '.format(symbol))
    print('Loading: ')
        
    data = data.json()
        
    if 'Error Message' in data:
        raise ApiExpception('Alpha vantage stock API failed with downloading data. Possibly wrong symbol name.')

    data = data['Time Series (Daily)'] #receving data starting from date, description is deleted
    print('Done loading json format for stock {}.'.format(symbol))

    stock_values = pd.DataFrame(columns={1, 2}) 
    stock_values = stock_values.rename(columns={1:'Date', 2: symbol})
    
    for d, p in data.items(): # looping over a dictionary 
        date = datetime.strptime(d,'%Y-%m-%d').date() # setting the key as datetime date
        data_row = [date, float(p['5. adjusted close'])] # creating rows only for the needed column
        stock_values.loc[-1,:] = data_row # adding values to before created data frame
        stock_values.index = stock_values.index + 1 
    stock_values = stock_values.sort_values('Date')
    
    
    stock_values['Date'] = pd.to_datetime(stock_values['Date'])
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    stock_values = stock_values.loc[(stock_values['Date'] >= start_date) & (stock_values['Date'] <= end_date)] # choosing the necessary sample period 
    
    stock_values = stock_values.set_index('Date')

    return stock_values


def load_data_fx(currency, start_date, end_date): 
    data = requests.get('https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol={}&outputsize=full&apikey={}'.format(currency, api_key))
    
    print('Data loading json format for FX USD/{}: '.format(currency))
    print('Loading: ')
    data = data.json()
    if 'Error Message' in data:
        raise ApiExpception('Alpha vantage currency API failed with downloading data. Possibly wrong currency name.')

    data = data['Time Series FX (Daily)'] 
    
    print('Done loading json format for FX USD/{}.'.format(currency))

    fx_values = pd.DataFrame(columns={1,2}) 
    fx_values = fx_values.rename(columns={1:'Date', 2:'USD/'+currency})

    for d, p in data.items():
        date = datetime.strptime(d,'%Y-%m-%d')
        data_row = [date, float(p['4. close'])]
        fx_values.loc[-1,:] = data_row
        fx_values.index = fx_values.index + 1 
    fx_values = fx_values.sort_values('Date')
    
    fx_values['Date'] = pd.to_datetime(fx_values['Date'])
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    fx_values = fx_values.loc[(fx_values['Date'] >= start_date) & (fx_values['Date']<= end_date)] 
    
    fx_values = fx_values.set_index('Date')
    
    return fx_values
    
def model(symbol, currency, start_date, end_date):  
    print('Loading stock data: ')
    stock = load_data_stock(symbol, start_date, end_date) # installing stock data
    print('Finished.')
    
    print('Loading fx data: ')
    fx = load_data_fx(currency, start_date, end_date) # installing fx data
    print('Finished.')
    
    dataframe = pd.merge(stock, fx, how = 'inner', left_index = True, right_index = True) # merging stock and fx data
    dataframe['Symbol'] = symbol # creating rows with stock symbol
    dataframe['Currency'] = currency # creating rows with currency symbol
    dataframe['Amount'] = dataframe[symbol] * dataframe['USD/' + currency] # calculating the deminonation in requested currency 
    dataframe = dataframe.drop([symbol,'USD/'+ currency], axis = 1) # dropping unnecessary columns 
    
    print('Value of stock finished calculating in {} currency'. format(currency)) 
     
    return dataframe
    



def createDbConnection(): # creating database
    engine = create_engine('sqlite:///foo_trial.db')
    return engine

def create_stocks_table(engine): # creating table to insert already used data
    meta = MetaData(engine)
    
    stocks = Table( # table creation
       'stocks', 
        meta, 
        Column('id', Integer, primary_key = True), 
        Column('Symbol', String), 
        Column('Currency', String), 
        Column('Date', Date),
        Column('Amount', String)
    )
    meta.create_all()
    return stocks 

def running_query(symbol, currency, start_date, end_date): # running query to pull out the necessary data
    query = stock_table.select().where(
        and_(
            stock_table.columns.Date >= pd.to_datetime(start_date),
            stock_table.columns.Date <= pd.to_datetime(end_date) + timedelta(days=1),
            stock_table.columns.Symbol == symbol,
            stock_table.columns.Currency == currency
        )
    )
    return pd.read_sql(query, conn)


def checking_api(symbol, currency, start_date, end_date, query_data):
    if query_data.empty: # if query is empty, pick all data from API
        stored = model(symbol, currency, start_date, end_date)
    
    else:
        min_value = query_data['Date'].min() # finding minimum value 
        max_value = query_data['Date'].max() # finding maximum value
        
        if min_value > pd.to_datetime(start_date): # if minimum date in query BIGGER than start_date
            print('Getting additional data from API before the earliest date in database.')
            new_end_date = min_value - timedelta(days=1)
            store_min = model(symbol, currency, start_date, new_end_date)
        else:
            store_min = pd.DataFrame([])
        
        if max_value < pd.to_datetime(end_date): # if maximum value in query smaller than end_date
            print('Getting additional data from API after the latest date in database.')
            new_start_date = max_value + timedelta(days=1)
            store_max = model(symbol, currency, new_start_date, end_date) 
        else:
            store_max = pd.DataFrame([])
    
        stored = pd.concat([store_min, store_max])
    
    stored.to_sql('stocks', conn, if_exists='append')
    return stored


def get_user_input():
    if(api_key == ''):
        raise UserInputException('No existing API key. Try again.')     

    symbol = input('Enter stock symbol: ').strip().upper()
    currency = input('Enter requested currency: ').strip().upper()
    
    start_date = input('Enter starting date YYYY-MM-DD: ')
    try: 
        start_date = pd.to_datetime(start_date)
    except Exception:
        raise UserInputException('Start date is invalid. Try again.')

    end_date = input('Enter ending date YYYY-MM-DD: ')
    try: 
        end_date = pd.to_datetime(end_date)
    except Exception:
        raise UserInputException('End date is invalid. Try again.')

    if pd.isna(start_date):
        raise UserInputException('Start date is empty. Try again.')

    if pd.isna(end_date):
        raise UserInputException('End date is empty. Try again.')

    if(start_date > end_date):
        raise UserInputException('End date has to be greater than start date. Please insert correct date parameters.')

    return symbol, currency, start_date, end_date    
    

def requesting_data():

    try:    
        symbol, currency, start_date, end_date = get_user_input()
    except UserInputException as e:
        print(str(e))
        return 


    print('Taking data from the database: ')
    query_data = running_query(symbol, currency, start_date, end_date) # pulling data from query
    query_data = query_data.drop(['id'],axis=1)
    print('Done.')
    
    print('Downloading data from Alpha Vangtage, if needed: ')
    try:
        api_data = checking_api(symbol, currency, start_date, end_date, query_data)
        print('Finished downloading API data.')


        query_data = query_data.set_index('Date') 
        dataset = pd.concat([query_data, api_data])
        dataset = dataset.sort_index()

        min_data = dataset.index.min()
        max_data = dataset.index.max()
        
        if min_data != start_date:
            print('Can not find the start date. Getting the earliest information.') 
        if max_data != end_date:
            print('Can not find the end date. Getting the latest information.') 

        dataset.index = pd.to_datetime(dataset.index)
        dataset.to_csv('data.csv')   
        print('{} value downloaded in {} currency'.format(symbol, currency))
        return dataset
    except ApiExpception as error:
        print('Try again: ')
        print(str(error))
        return
        
engine = createDbConnection() # running the engine 
conn = engine.connect() # making connection to the engine 
stock_table = create_stocks_table(engine) # creating table as named stocks

api_key = ''
requesting_data()
