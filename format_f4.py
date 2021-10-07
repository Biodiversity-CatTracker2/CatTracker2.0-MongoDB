import pandas as pd
import datetime as dt


def f4_formatting(file, start: '%d/%m/%Y %H:%M:%S', end: '%d/%m/%Y %H:%M:%S'):
    '''
    Description: Formats raw ACC data to a format that Framework4 understands.
    file: csv file exported from Cat_Lora_Proj.exe with the ACC data.
    start: date and time of the desired start point.
    end: date and time of the desired end point.
    '''
    df = pd.read_csv(file)

    df['Date'] = pd.to_datetime(start[:10] + ' ' + df['Time']).apply(
        lambda x: dt.datetime.strftime(x, '%d/%m/%Y %H:%M:%S'))

    df = df.loc[(df['Date'] >= start) & (df['Date'] <= end)]

    df['Date'] = df['Date'].apply(lambda x: dt.datetime.strptime(
        x, '%d/%m/%Y %H:%M:%S')).dt.tz_localize('UTC')

    M = {'dMin': [], 'uMin': []}

    for x in df['Date'].dt.minute:

        if len(str(x)) == 2:
            M['dMin'].append(int(f'{str(x)[0]}') * 10)
            M['uMin'].append(int(f'{str(x)[1]}'))

        else:
            M['dMin'].append(0), M['uMin'].append(x)

    df = df.assign(H=df['Date'].dt.hour,
                   Sec=df['Date'].dt.second,
                   dMin=M['dMin'],
                   uMin=M['uMin'])  #, event=df['Eventnum']
    df = df[['Accx', 'Accy', 'Accz', 'Sec', 'uMin', 'dMin', 'H', 'event']]

    df.to_csv(f'{file}_DataFR4.csv', index=False)
