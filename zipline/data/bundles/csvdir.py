"""
Module for building a complete dataset from local directory with csv files.
V1 initial version
v2 ability to set Exchange and AssetType
"""
import os
import sys

import logbook
from numpy import empty
from pandas import DataFrame, read_csv, Index, Timedelta, NaT

from zipline.utils.calendars import register_calendar_alias
from zipline.utils.cli import maybe_show_progress

logger = logbook.Logger(__name__)
exchanges =['CSVDIR']


def csvdir_equities(tframes=['daily'], start=None, end=None):
    """
    Generate an ingest function for custom data bundle

    Parameters
    ----------
    tframe: list or tuple, optional
        The data time frames ('minute', 'daily')
    start : datetime, optional
        The start date to query for. By default this pulls the full history
        for the calendar.
    end : datetime, optional
        The end date to query for. By default this pulls the full history
        for the calendar.
    Returns
    -------
    ingest : callable
        The bundle ingest function for the given set of symbols.
    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle',
                csvdir_equities(sys.environ['CSVDIR'],
                ['daily', 'minute']))

    Notes
    -----
    Environment variable CSVDIR must contain path to the directory with the
    following structure:
        daily/<symbol>.cvs files
        minute/<symbol>.csv files
        
    or 
        daily/<symbol>_<exchange>.cvs files
        minute/<symbol>_<exchange>.csv files
        
    or 
        daily/<symbol>_<exchange>_<assettype>.cvs files
        minute/<symbol>_<exchange>_<assettype>.csv files              
    
    for each symbol.
    
    One can add the exchange and asset type only when needed, eg. GLD needs 
    ARCA to trade or to get pricing data from IB instead of the default SMART.
    VIX needs CBOE as exchange and IND as asset type to get data. 
    Exchanges are also mandatory for futures (not supported yet)
    
    """

    return CSVDIRBundle(tframes, start, end).ingest


class CSVDIRBundle:
    """
    Wrapper class to enable write access to self.splits and self.dividends
    from _pricing_iter method.
    """

    def __init__(self, tframes, start, end):
        self.tframes = tframes
        self.start = start
        self.end = end

        self.show_progress = None
        self.symbols = None
        self.metadata = None
        self.csvdir = None

        self.splits = None
        self.dividends = None
        self.default_exchange = "CSVDIR"
        self.default_asset_type = "STK"
        self.exchanges = {}
        self.assettypes = {}
        self.csvfiles = {}


    def ingest(self, environ, asset_db_writer, minute_bar_writer,
               daily_bar_writer, adjustment_writer, calendar, start_session,
               end_session, cache, show_progress, output_dir):

        csvdir = os.environ.get('CSVDIR')
        if not csvdir:
            logger.error("CSVDIR environment variable is not set")
            sys.exit(1)

        if not os.path.isdir(csvdir):
            logger.error("%s is not a directory" % csvdir)
            sys.exit(1)

        for tframe in self.tframes:
            ddir = os.path.join(csvdir, tframe)
            if not os.path.isdir(ddir):
                logger.error("%s is not a directory" % ddir)

        for tframe in self.tframes:
            ddir = os.path.join(csvdir, tframe)
            self.symbols =[]
            self.csvfiles={}
            for item in os.listdir(ddir):
                if item.endswith('.csv'):
                    file = item
                    symbol = item.split('.csv')[0].split('_')
                    if len(symbol)>1:
                        #The exchange was in the file name
                        self.exchanges[symbol[0]]=symbol[1]
                        if len(symbol)>2:
                            self.assettypes[symbol[0]]=symbol[2]
                    symbol=symbol[0]    
                    self.symbols.append(symbol)
                    self.csvfiles[symbol]=file
            self.symbols.sort()
             
            if not self.symbols:
                logger.error("no <symbol>.csv files found in %s" % ddir)
                sys.exit(1)

            self.csvdir = ddir

            dtype = [('start_date', 'datetime64[ns]'),
                     ('end_date', 'datetime64[ns]'),
                     ('auto_close_date', 'datetime64[ns]'),
                     ('symbol', 'object'), 
                     ('exchange','object'),
                     ('exchange_full', 'object')]
            #TODO ASSET TYPE
            self.metadata = DataFrame(empty(len(self.symbols), dtype=dtype))

            self.show_progress = show_progress

            if tframe == 'minute':
                writer = minute_bar_writer
            else:
                writer = daily_bar_writer

            writer.write(self._pricing_iter(), show_progress=show_progress)

#             # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
#             # register "CSVDIR" to resolve to the NYSE calendar, because these
#             # are all equities and thus can use the NYSE calendar.
#             self.metadata['exchange'] = "CSVDIR"

        asset_db_writer.write(equities=self.metadata)

        adjustment_writer.write(splits=self.splits, dividends=self.dividends)

    def _pricing_iter(self):
        with maybe_show_progress(self.symbols, self.show_progress,
                                 label='Loading custom pricing data: ') as it:
            for sid, symbol in enumerate(it):
                logger.debug('%s: sid %s' % (symbol, sid))

                dfr = read_csv(os.path.join(self.csvdir, self.csvfiles[symbol]),
                               parse_dates=[0], infer_datetime_format=True,
                               index_col=0).sort_index()

                # the start date is the date of the first trade and
                # the end date is the date of the last trade
                start_date = dfr.index[0]
                end_date = dfr.index[-1]

                # The auto_close date is the day after the last trade.
                ac_date = end_date + Timedelta(days=1)
                exchange = self.default_exchange if symbol not in self.exchanges else self.exchanges[symbol]
                exchange_full = self.default_asset_type if symbol not in self.assettypes else self.assettypes[symbol]        
                self.metadata.iloc[sid] = start_date, end_date, ac_date, symbol, exchange, exchange_full
                
                if 'split' in dfr.columns:
                    if self.splits is None:
                        self.splits = DataFrame()
                    tmp = dfr[dfr['split'] != 1.0]['split']
                    split = DataFrame(data=tmp.index.tolist(),
                                      columns=['effective_date'])
                    split['ratio'] = tmp.tolist()
                    split['sid'] = sid

                    index = Index(range(self.splits.shape[0],
                                        self.splits.shape[0] + split.shape[0]))
                    split.set_index(index, inplace=True)
                    self.splits = self.splits.append(split)

                if 'dividend' in dfr.columns:
                    if self.dividends is None:
                        self.dividends = DataFrame()
                    # ex_date   amount  sid record_date declared_date pay_date
                    tmp = dfr[dfr['dividend'] != 0.0]['dividend']
                    div = DataFrame(data=tmp.index.tolist(),
                                    columns=['ex_date'])
                    div['record_date'] = NaT
                    div['declared_date'] = NaT
                    div['pay_date'] = NaT
                    div['amount'] = tmp.tolist()
                    div['sid'] = sid
                    ind = Index(range(self.dividends.shape[0],
                                      self.dividends.shape[0] + div.shape[0]))
                    div.set_index(ind, inplace=True)
                    if self.dividends is None:
                        self.dividends = DataFrame()
                    self.dividends = self.dividends.append(div)

                yield sid, dfr
                
        exchanges.extend(list(set(self.exchanges.values())))

exchanges = list(set(exchanges))
for exchange in exchanges:
    register_calendar_alias(exchange, "NYSE")
