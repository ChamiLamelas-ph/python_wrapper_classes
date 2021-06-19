"""
--- DEPRECATED 4/2/2021 --- 

This file contains a collection of classes used for pulling daily order statistics from Amazon. 

The primary user class is AmazonOrderRetrieval. The other classes are used for implementation of this class.
"""
# Chami Lamelas
# Last updated 2/9/2021

import pytz
from tqdm import tqdm
from mws import mws
from enum import Enum
from datetime import datetime,timedelta
from collections import defaultdict
import pandas as pd
import xmltodict

class AmazonOrderRetrievalStatus(Enum):
    """
    This enum holds the possible statuses of an order retrieval request sent via AmazonOrderRetrieval.

    With the exception of `REQUEST_FAILED`, the instances of this enum will be used to return the status of a call to retrieve_orders() in AmazonOrderRetrieval. The name of `REQUEST_FAILED` is used to set the `processing_status` field of the AmazonReportLogEntry class when no other processing status can be retrieved from Amazon.
    
    See Also
    --------
    AmazonOrderRetrieval
    AmazonReportLogEntry
    """

    REQUEST_FAILED = 1
    """
    This status means that a request to generate a report has failed.
    """

    CREATED_REQUEST = 2
    """
    This status means that a request to generate a report was created successfully. 
    """

    REQUEST_PROCESSING = 3
    """
    This status means that the request for a report has not completed.
    """

    SAVED_ORDERS = 4
    """
    This status means that the orders were retrieved and saved by AmazonOrderRetrieval.
    """

    ORDERS_ALREADY_SAVED = 5
    """
    This status means that the orders associated with the request have already been saved via AmazonOrderRetrieval.
    """

class AmazonOrderSavingStatus(Enum):
    """
    This enum is used to hold the possible statuses of saving orders from a report via AmazonOrderRetrieval. 

    The names of these instances are used to set the `saving_status` of AmazonReportLogEntry objects sent to and from the client code of AmazonOrderRetrieval.

    See Also
    --------
    AmazonOrderRetrieval
    AmazonReportLogEntry
    """

    NOT_SAVED = 1
    """
    This means that no attempt has been made to save a set of orders.
    """

    SAVE_FAILED = 2
    """
    This means that the last attempt to save a set of orders failed. The reason it has failed will be included in the `last_err_type` field of AmazonReportLogEntry.
    """

    EMPTY_SAVE = 3
    """
    This means that the last attempt to save a set of orders resulted in an empty save. The client code is able to make additional calls to retrieve_orders() in this case.
    """

    SAVED = 4
    """
    This means that the last attempt to save orders was successful and resulted in a non-empty save. 
    """

class AmazonReportLogEntry:
    """
    This class serves to consolidate logging information to be transferred between AmazonOrderRetrieval and the client code.

    The transfer will be implemented as a subclass of AmazonReportLogger. 

    Attributes
    ----------
    submitted_date : str or datetime
        This is the time at which a report was requested.
    request_id : str
        This is the ID of the request.
    report_id : str
        This is the ID of the report.
    day : str
        This is the yyyy-MM-dd representation of the date the report was requested for. 
    marketplace : str
        This is the marketplace of the date the report was requested for. 
    processing_status : str
        This is the current processing status either reported from Amazon or `REQUEST_FAILED` as discussed above.
    saving_status : str
        This is the current saving status of the order data from this report.
    last_err_type : str
        This is the type of the last error generated in processing/saving this report.

    Parameters
    ----------
    submitted_date : str or datetime
        This is the time at which a report was requested.
    request_id : str
        This is the ID of the request.
    report_id : str
        This is the ID of the report.
    day : str
        This is the yyyy-MM-dd representation of the date the report was requested for. 
    marketplace : str
        This is the marketplace of the date the report was requested for. 
    processing_status : str
        This is the current processing status either reported from Amazon or `REQUEST_FAILED` as discussed above.
    saving_status : str
        This is the current saving status of the order data from this report.
    last_err_type : str
        This is the type of the last error generated in processing/saving this report.

    Warning
    -------
    When transfering data from your logging system via AmazonReportLogger into these objects, make sure that all of the information you pass in are strings even though you may be storing fields (like IDs) as other types. 

    See Also
    --------
    AmazonOrderRetrieval
    AmazonReportLogger
    AmazonOrderRetrievalStatus
    AmazonOrderSavingStatus
    """

    def __init__(self,submitted_date,request_id,report_id,day,marketplace,processing_status,saving_status,last_err_type):
        self.submitted_date = submitted_date
        self.request_id = request_id
        self.report_id = report_id
        self.day = day
        self.marketplace = marketplace
        self.processing_status = processing_status
        self.saving_status = saving_status
        self.last_err_type = last_err_type

    def deepcopy(self):
        """
        This function provides a deep copy of an AmazonReportLogEntry object. 

        Returns
        -------
        AmazonReportLogEntry
            A deep copy of this object.
        """

        return AmazonReportLogEntry(
            self.submitted_date,
            self.request_id,
            self.report_id,
            self.day,
            self.marketplace,
            self.processing_status,
            self.saving_status,
            self.last_err_type
        )

class AmazonReportLogger:
    """
    This class provides a template for subclasses which should implement some non-volatile logging system.

    This class will be used by AmazonOrderRetrieval to send and receive information from your logging system. This could be writing to a database or a file. A constructor is omitted from the template, but you should add one if you need to perform any set-up operations for your logging system.
    """

    def log_info(self,log_entry):
        """
        This function should log the information from an AmazonReportLogEntry object to your system.

        The information passed through this function should not be edited in anyway. When a request is made via get_info() to get this information back, it should be exactly the same. However, in this method, you can add any other information to your logging system.

        Parameters
        ----------
        log_entry : AmazonReportLogEntry
            A log entry to be put into your system. 

        Notes
        -----
        This function will be called to update information in your system throughout AmazonOrderRetrieval's retrieve_orders() method to keep you updated with the progress of your report processing.

        See Also
        --------
        AmazonOrderRetrieval
        AmazonReportLogEntry
        get_info
        """
        pass

    def get_info(self,day,marketplace):
        """
        This function should retrieve a log entry object with the necessary information associated with a day and marketplace. 

        The necessary information should be all the fields in an AmazonReportLogEntry object that would have been passed in via a call to log_info().

        Parameters
        ----------
        day : str
            The day of interest.
        marketplace : str
            The marketplace of interest.

        Returns
        -------
        AmazonReportLogEntry
            An object containing the necessary information for this (day,marketplace).

        Notes
        -----
        This function will be called to retrieve information about a report request associated with day and marketplace logged in your system throughout AmazonOrderRetrieval's retrieve_orders() method so that it can act accordingly.

        See Also
        --------
        AmazonOrderRetrieval
        AmazonReportLogEntry
        log_info
        """
        pass

class AmazonOrderRetrieval:
    """
    This class is the primary class in this library and is used for retrieving the orders for a day. 

    Its primary method is retrieve_orders() which will retrieve the ASINs, product names, orders, and revenue totals for each SKU on a particular day and marketplace. retrieve_orders() requests and reads a report of your orders into a customized saving system. 

    Parameters
    ----------
    access_key : str
        MWS access key.
    secret_key : str
        MWS secret key.
    account_id : str
        MWS account (or seller) ID.
    rates : dict
        Dictionary that should map a (day,currency) tuple to a rate. day should be a yyyy-MM-dd date and the currency should be an Amazon recognized currency (included in see also).
    logger : AmazonReportLogger
        An instance of a subclass of AmazonReportLogger that implements your logging system. For more information on how this should be implemented, please read the docs on AmazonReportLogger.
    marketplaces : dict
        Dictionary that should map how you plan to refer to a marketplace to its marketplace ID. The keys and values in this dictionary should both be strings. 
    save_orders : function
        Function that takes a pandas DataFrame, does some preprocessing (adding, renaming columns, rounding revenue etc.), and saves it in some non-volatile saving system. The DataFrame will have the following columns: marketplace (same as `marketplace`), date (same as `day`), sku (the string of a particular SKU), asins (a set with all of the ASINs associated with this SKU on this `day` in `marketplace`), product_names (a set with all of the product names associated with this SKU on this `day` and `marketplace`), order_count (number of orders for this `SKU` on `day` and `marketplace`), these columns will be followed by revenue columns for all revenue types detected for this `day` and `marketplace`. These will be named according to Amazon's conventions. If some SKUs do not have all of the revenue types, it will have 0 *not* NULL for those revenue types.
    
    Notes
    -----
    This class is built on top of the MWS Python implementation. In future, it may get updated to the newer sp-api. The current used report type is `_GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_`.

    See Also
    --------
    AmazonReportLogger
    retrieve_orders

    References
    ----------
    [Supported Amazon Currencies](https://www.amazon.com/gp/help/customer/display.html/ref=s9_acss_bw_cg_ACCBLand_3a1_w?nodeId=201894850&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-2&pf_rd_r=FYEXRCKKHT3KFPERXRD0&pf_rd_t=101&pf_rd_p=82bf0150-d0c9-411f-982f-b0c577129b1c&pf_rd_i=388305011)

    [Amazon Order Report Types](http://docs.developer.amazonservices.com/en_US/reports/Reports_ReportType.html#ReportTypeCategories__OrderReports)

    [MWS on PyPi](https://pypi.org/project/mws/)

    [MWS on GitHub](https://github.com/python-amazon-mws)
    """

    # Report type used to retrieve orders
    __REPORT_TYPE = "_GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_" 
    
    def __init__(self, access_key, secret_key, account_id, rates, logger, marketplaces, save_orders):
        self.__reports_access = mws.Reports(
            access_key=access_key, 
            secret_key=secret_key, 
            account_id=account_id
        )

        self.__rates = rates
        self.__logger = logger
        self.__marketplaces = marketplaces
        self.__save_orders = save_orders

    def retrieve_orders(self,day,marketplace):
        """
        This method will retrieve the orders for a particular day and marketplace. 

        This method can provide a variety of responses. 

        Parameters
        ----------
        day : str
            The yyyy-MM-dd date of interest.
        marketplace : str
            The name for the marketplace. Its ID should be retrievable from `marketplaces`.

        Returns
        -------
        AmazonOrderRetrievalStatus
            This status is returned in the event that the report request can be successfully accessed. `AmazonOrderRetrievalStatus.CREATED_REQUEST` is returned if a request could be created successfully and its information could be logged using `logger`. `AmazonOrderRetrievalStatus.ORDERS_ALREADY_SAVED` is returned to signal the method does not thing because orders have already been saved via `save_orders`. `AmazonOrderRetrievalStatus.REQUEST_PROCESSING` is returned to signal that the method checked the status of the report for the `day` and `marketplace` and it is not finished yet. Its most recent processing status is updated and sent via `logger`. `AmazonOrderRetrievalStatus.SAVED_ORDERS` is returned if a report could be read and its order information could be saved successfully using `save_orders` and logged.

        Raises
        ------
        MWSError
            These errors are raised in the event of failures of report requests via the MWS package. This can occur due to requests being throttled or if you have made too many creation requests recently (17 / 5 minutes). If such an error occurs while making a report reuest, then the processing status is set as the name of `AmazonOrderRetrievalStatus.REQUEST_FAILED`, and submission time of the current PST time, and logged via `logger`. If such an error occurs during saving, then the saving status is set as the name of `AmazonOrderSavingStatus.SAVE_FAILED`.
        Exception
            Any other exceptions that may occur can also be raised. However, if they occur in request creation, progress update, or order saving their type (which may be MWSError) will be logged in the `logger`. If the exception occurred during request creation or saving, the processing and saving statuses will be updated as above.

        Notes
        -----
        Whenever information needs to be updated in your system in this method, AmazonReportLogEntry's are copied from your system, edited, and passed back via AmazonReportLogger. Any information that needs to be retrieved is requested from AmazonReportLogger as AmazonReportLogEntry. The error type logged in your system is an indication of the type of error that happened and the method should be re-run to actually see a full traceback of the error.

        See Also
        --------
        AmazonReportLogger
        AmazonReportLogEntry
        AmazonOrderRetrievalStatus
        AmazonOrderSavingStatus

        References
        --------
        [RequestReport Operation](https://docs.developer.amazonservices.com/en_US/reports/Reports_RequestReport.html)

        [GetReportRequestList Operation](https://docs.developer.amazonservices.com/en_US/reports/Reports_GetReportRequestList.html)

        [GetReport Operation](https://docs.developer.amazonservices.com/en_US/reports/Reports_GetReport.html)
        """

        next_day = (datetime.strptime(day,"%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        marketplace_id = self.__marketplaces[marketplace]
        log_entry = self.__logger.get_info(day,marketplace)

        if log_entry is None or log_entry.processing_status == AmazonOrderRetrievalStatus.REQUEST_FAILED.name:

            # make order request for this date range and market place and collect request response
            try:
              request_response = xmltodict.parse(self.__reports_access.request_report(AmazonOrderRetrieval.__REPORT_TYPE, day, next_day, marketplace_id).original)
              request_response = request_response['RequestReportResponse']['RequestReportResult']['ReportRequestInfo']
              request_info = xmltodict.parse(self.__reports_access.get_report_request_list(request_response['ReportRequestId']).original)
              request_info = request_info['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']
            except Exception as e:
              curr_pst_time = datetime.now(pytz.timezone('America/Los_Angeles'))
              self.__logger.log_info(AmazonReportLogEntry(
                  curr_pst_time,
                  None,
                  None,
                  day,
                  marketplace,
                  AmazonOrderRetrievalStatus.REQUEST_FAILED.name,
                  AmazonOrderSavingStatus.NOT_SAVED.name,
                  str(type(e))
              ))
              raise e

            # create log for this request using the information collected in request info via GetReportRequestList
            self.__logger.log_info(AmazonReportLogEntry(
                request_info['SubmittedDate'],
                request_info['ReportRequestId'],
                request_info['GeneratedReportId'] if request_info['ReportProcessingStatus'] == '_DONE_' else None,
                day,
                marketplace,
                request_info['ReportProcessingStatus'],
                AmazonOrderSavingStatus.NOT_SAVED.name,
                None
            ))
            return AmazonOrderRetrievalStatus.CREATED_REQUEST

        if log_entry.saving_status == AmazonOrderSavingStatus.SAVED.name:
            return AmazonOrderRetrievalStatus.ORDERS_ALREADY_SAVED

        log_entry = log_entry.deepcopy()
        log_entry.last_err_type = None

        if log_entry.processing_status != '_DONE_':
            # get most recent request info using request id from log information (log information could be old)
            try: 
              request_info = xmltodict.parse(self.__reports_access.get_report_request_list(log_entry.request_id).original)
              request_info = request_info['GetReportRequestListResponse']['GetReportRequestListResult']['ReportRequestInfo']            
              log_entry.processing_status = request_info['ReportProcessingStatus']
              if request_info['ReportProcessingStatus'] != '_DONE_':
                  return AmazonOrderRetrievalStatus.REQUEST_PROCESSING
              log_entry.report_id = request_info['GeneratedReportId']
            except Exception as e:
              log_entry.last_err_type = str(type(e))
              raise e   
            finally:
              self.__logger.log_info(log_entry)

        try:
            # retrieve orders using GetReport with the generated ID (which will now exist because report was created)
            orders = xmltodict.parse(self.__reports_access.get_report(log_entry.report_id).original)
            df = None
            if 'Message' in orders['AmazonEnvelope']:
              orders = orders['AmazonEnvelope']['Message']
              df = self.__build_df(day, marketplace, orders) # build dataframe of order stats 
              if df is not None:
                self.__save_orders(df)
                log_entry.saving_status = AmazonOrderSavingStatus.SAVED.name
            if df is None:
              log_entry.saving_status = AmazonOrderSavingStatus.EMPTY_SAVE.name
            return AmazonOrderRetrievalStatus.SAVED_ORDERS
        except Exception as e:
            log_entry.saving_status = AmazonOrderSavingStatus.SAVE_FAILED.name
            log_entry.last_err_type = str(type(e))
            raise e
        finally:
            self.__logger.log_info(log_entry)

    def __update_price_component(self, dataset, order_date, sku, price_component):
        currency = price_component['Amount']['@currency'] # get currency   

        # if price already in USD, no change, else get the conversion to USD from rates dictionary for the date
        rate = 1.0
        if currency != 'USA':
          rate = self.__rates[(order_date, currency)]

        # revenue type will be amazon provided type name followed by '_revenue' in database
        revenue_type = price_component['Type'].lower() + '_revenue'       
        dataset[sku][revenue_type] += float(price_component['Amount']['#text']) * rate

    def __update_dataset(self, dataset, order_date, order_item):
        # When 'ItemStatus' isn't present, seems to be a quantity of 0, non-shipped items have no item price
        if 'ItemStatus' not in order_item or order_item['ItemStatus'] != 'Shipped': 
            return

        sku = order_item['SKU'] 
        dataset[sku]['asins'].add(order_item['ASIN']) # add ASIN for current item
        dataset[sku]['product_names'].add(order_item['ProductName']) # add product name for current item
        dataset[sku]['order_count'] += int(order_item['Quantity']) # add quantity of item ordered

        price_components = order_item['ItemPrice']['Component']
        if isinstance(price_components, list):
          for pc in price_components:
            self.__update_price_component(dataset, order_date, sku, pc)
        else:
          self.__update_price_component(dataset, order_date, sku, price_components)

    def __update_from_items(self, dataset, date, items):
        if isinstance(items, list): # multiple items, it's a list of dicts
            for item in items:
                self.__update_dataset(dataset, date, item)
        else: # otherwise, it's a dict
            self.__update_dataset(dataset, date, items)


    def __build_df(self, date, marketplace, orders):
        # collect information in a dictionary for each SKU
        dataset = defaultdict(lambda: 
            defaultdict(lambda: 0, {
                'asins':set(),
                'product_names':set(),
                'order_count':0
            })
        ) 

        if isinstance(orders,list):
          for o in tqdm(orders, desc='Processing Orders...', total=len(orders)):
              self.__update_from_items(dataset, date, o['Order']['OrderItem'])
        else:
          self.__update_from_items(dataset, date, orders['Order']['OrderItem'])
                    
        if len(dataset)==0:
          return None

        # load dataset dictionary into dataframe, keys of dictionary will be rows (index)
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.from_dict.html
        df = pd.DataFrame.from_dict(dataset, orient='index')
        # want SKU to be a column, reset_index() to create numerical index 
        # reseting index puts sku into 'index', rename it
        df = df.reset_index().rename(columns={'index':'sku'}) 
        # some skus don't have all revenue types and will have nulls there, replace them with 0s
        df = df.fillna(0) 
        # add date, marketplace columns
        df['marketplace'] = marketplace 
        df['date'] = date
        # reorder columns putting marketplace, date first
        cols = list(df.columns) 
        df = df[cols[-2:]+cols[:-2]]       
        return df
