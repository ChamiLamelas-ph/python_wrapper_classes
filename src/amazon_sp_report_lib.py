# https://python-amazon-sp-api.readthedocs.io/en/v0.3.1/endpoints/reports/
# https://github.com/amzn/selling-partner-api-docs/blob/main/references/reports-api/reportType_string_array_values.md

from amazon_sp_constants import NA_MARKETPLACE_COUNTRY_CODESET, EUR_MARKETPLACE_COUNTRY_CODESET
from sp_api.base import Marketplaces, SellingApiException
from sp_api.api import Reports
from datetime import datetime
from enum import Enum
from io import StringIO
import pandas as pd
import chardet
import pickle as pkl
import re
import time

TAB_REPORT_TYPES = {
    'Inventory Report': 'GET_FLAT_FILE_OPEN_LISTINGS_DATA',
    'All Listings Report': 'GET_MERCHANT_LISTINGS_ALL_DATA',
    'Active Listings Report': 'GET_MERCHANT_LISTINGS_DATA',
    'Inactive Listings Report': 'GET_MERCHANT_LISTINGS_INACTIVE_DATA',
    'Open Listings Report': 'GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT',
    'Open Listings Report Lite': 'GET_MERCHANT_LISTINGS_DATA_LITE',
    'Open Listings Report Liter': 'GET_MERCHANT_LISTINGS_DATA_LITER',
    'Canceled Listings Report': 'GET_MERCHANT_CANCELLED_LISTINGS_DATA',
    'Listing Quality and Suppressed Listing Report': 'GET_MERCHANT_LISTINGS_DEFECT_DATA',
    'Pan-European Eligibility: FBA ASINs': 'GET_PAN_EU_OFFER_STATUS',
    'Pan-European Eligibility: Self-fulfilled ASINs': 'GET_MFN_PAN_EU_OFFER_STATUS',
    'Global Expansion Opportunities Report': 'GET_FLAT_FILE_GEO_OPPORTUNITIES',
    'Referral Fee Preview Report': 'GET_REFERRAL_FEE_PREVIEW_REPORT',
    'Unshipped Orders Report': 'GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING',
    'Requested or Scheduled Flat File Order Report (Invoicing)': 'GET_FLAT_FILE_ORDER_REPORT_DATA_INVOICING',
    'Requested or Scheduled Flat File Order Report (Shipping)': 'GET_FLAT_FILE_ORDER_REPORT_DATA_SHIPPING',
    'Requested or Scheduled Flat File Order Report (Tax)': 'GET_FLAT_FILE_ORDER_REPORT_DATA_TAX',
    'Flat File Orders By Last Update Report': 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL',
    'Flat File Orders By Order Date Report': 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
    'Flat File Archived Orders Report': 'GET_FLAT_FILE_ARCHIVED_ORDERS_DATA_BY_ORDER_DATE',
    'Flat File Pending Orders Report': 'GET_FLAT_FILE_PENDING_ORDERS_DATA',
    'Flat File Returns Report by Return Date': 'GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE',
    'Flat File Return Attributes Report by Return Date': 'GET_FLAT_FILE_MFN_SKU_RETURN_ATTRIBUTES_REPORT',
    'Flat File Feedback Report': 'GET_SELLER_FEEDBACK_DATA',
    'Flat File Settlement Report': 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE',
    'Flat File V2 Settlement Report': 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2',
    'FBA Amazon Fulfilled Shipments Report': 'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL',
    'FBA Amazon Fulfilled Shipments Report (Invoicing)': 'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_INVOICING',
    'FBA Amazon Fulfilled Shipments Report (Tax)': 'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_TAX',
    'Flat File All Orders Report by Last Update': 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL',
    'Flat File All Orders Report by Order Date': 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
    'FBA Customer Shipment Sales Report': 'GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA',
    'FBA Promotions Report': 'GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA',
    'FBA Customer Taxes': 'GET_FBA_FULFILLMENT_CUSTOMER_TAXES_DATA',
    'Remote Fulfillment Eligibility': 'GET_REMOTE_FULFILLMENT_ELIGIBILITY',
    'FBA Amazon Fulfilled Inventory Report': 'GET_AFN_INVENTORY_DATA',
    'FBA Multi-Country Inventory Report': 'GET_AFN_INVENTORY_DATA_BY_COUNTRY',
    'Inventory Ledger Report - Summary View': 'GET_LEDGER_SUMMARY_VIEW_DATA',
    'Inventory Ledger Report - Detailed View': 'GET_LEDGER_DETAIL_VIEW_DATA',
    'FBA Daily Inventory History Report': 'GET_FBA_FULFILLMENT_CURRENT_INVENTORY_DATA',
    'FBA Monthly Inventory History Report': 'GET_FBA_FULFILLMENT_MONTHLY_INVENTORY_DATA',
    'FBA Received Inventory Report': 'GET_FBA_FULFILLMENT_INVENTORY_RECEIPTS_DATA',
    'FBA Reserved Inventory Report': 'GET_RESERVED_INVENTORY_DATA',
    'FBA Inventory Event Detail Report': 'GET_FBA_FULFILLMENT_INVENTORY_SUMMARY_DATA',
    'FBA Inventory Adjustments Report': 'GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA',
    'FBA Inventory Health Report': 'GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA',
    'FBA Manage Inventory': 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA',
    'FBA Manage Inventory - Archived': 'GET_FBA_MYI_ALL_INVENTORY_DATA',
    'Restock Inventory Report': 'GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT',
    'FBA Inbound Performance Report': 'GET_FBA_FULFILLMENT_INBOUND_NONCOMPLIANCE_DATA',
    'FBA Stranded Inventory Report': 'GET_STRANDED_INVENTORY_UI_DATA',
    'FBA Bulk Fix Stranded Inventory Report': 'GET_STRANDED_INVENTORY_LOADER_DATA',
    'FBA Inventory Age Report': 'GET_FBA_INVENTORY_AGED_DATA',
    'FBA Manage Excess Inventory Report': 'GET_EXCESS_INVENTORY_DATA',
    'FBA Storage Fees Report': 'GET_FBA_STORAGE_FEE_CHARGES_DATA',
    'Get Report Exchange Data': 'GET_PRODUCT_EXCHANGE_DATA',
    'FBA Fee Preview Report': 'GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA',
    'FBA Reimbursements Report': 'GET_FBA_REIMBURSEMENTS_DATA',
    'FBA Long Term Storage Fee Charges Report': 'GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA',
    'FBA Returns Report': 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA',
    'FBA Replacements Report': 'GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA',
    'FBA Recommended Removal Report': 'GET_FBA_RECOMMENDED_REMOVAL_DATA',
    'FBA Removal Order Detail Report': 'GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA',
    'FBA Removal Shipment Detail Report': 'GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA',
    'Small & Light Inventory Report': 'GET_FBA_UNO_INVENTORY_DATA',
    'Sales Tax Report': 'GET_FLAT_FILE_SALES_TAX_DATA',
    'Amazon VAT Transactions Report': 'GET_VAT_TRANSACTION_DATA',
    'On Demand GST Merchant Tax Report B2B': 'GET_GST_MTR_B2B_CUSTOM',
    'On Demand GST Merchant Tax Report B2C': 'GET_GST_MTR_B2C_CUSTOM',
    'EasyShip Picked Up Report': 'GET_EASYSHIP_PICKEDUP',
    'EasyShip Waiting for Pick Up Report': 'GET_EASYSHIP_WAITING_FOR_PICKUP'
}
"""
Report type names & values for tab report types.
"""

XML_REPORT_TYPES = {
    'Scheduled XML Order Report (Invoicing)': 'GET_ORDER_REPORT_DATA_INVOICING',
    'Scheduled XML Order Report (Tax)': 'GET_ORDER_REPORT_DATA_TAX',
    'Scheduled XML Order Report (Shipping)': 'GET_ORDER_REPORT_DATA_SHIPPING',
    'XML Orders By Last Update Report': 'GET_XML_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL',
    'XML Orders By Order Date Report': 'GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
    'XML Pending Orders Report': 'GET_PENDING_ORDERS_DATA',
    'XML Returns Report by Return Date': 'GET_XML_RETURNS_DATA_BY_RETURN_DATE',
    'XML Prime Returns Report by Return Date': 'GET_XML_MFN_PRIME_RETURNS_REPORT',
    'XML Return Attributes Report by Return Date': 'GET_XML_MFN_SKU_RETURN_ATTRIBUTES_REPORT',
    'XML Customer Metrics Report': 'GET_V1_SELLER_PERFORMANCE_REPORT',
    'XML Settlement Report': 'GET_V2_SETTLEMENT_REPORT_DATA_XML',
    'Browse Tree Report': 'GET_XML_BROWSE_TREE_DATA'
}
"""
Report type names & values for XML report types.
"""

CSV_REPORT_TYPES = {
    'CSV Prime Returns Report by Return Date': 'GET_CSV_MFN_PRIME_RETURNS_REPORT',
    'Amazon VAT Calculation Report': 'SC_VAT_TAX_REPORT',
    'AmazonPay Sandbox Settlement Report': 'GET_FLAT_FILE_OFFAMAZONPAYMENTS_SANDBOX_SETTLEMENT_DATA',
    'B2B Product Opportunities - Recommended for You Report': 'GET_B2B_PRODUCT_OPPORTUNITIES_RECOMMENDED_FOR_YOU',
    'B2B Product Opportunities - Not yet on Amazon': 'GET_B2B_PRODUCT_OPPORTUNITIES_NOT_YET_ON_AMAZON'
}
"""
Report type names & values for CSV report types.
"""

PDF_REPORT_TYPES = {
    'EasyShip Report': 'GET_EASYSHIP_DOCUMENTS'
}
"""
Report type names & values for PDF report types.
"""

XLSX_REPORT_TYPES = {
    'Manage Quotes Report': 'RFQD_BULK_DOWNLOAD',
    'Referral Fee Discounts Report': 'FEE_DISCOUNTS_REPORT'
}
"""
Report type names & values for Excel report types.
"""

UNKNOWN_REPORT_TYPES = {
    'Converged Flat File Pending Orders Report': 'GET_CONVERGED_FLAT_FILE_PENDING_ORDERS_DATA',
    'Seller Performance Report': 'GET_V2_SELLER_PERFORMANCE_REPORT'
}
"""
Report type names & values for unknown report types.
"""


class SpReportTrackingStatus(Enum):
    """
    Enum that represents the status of updates made to the tracker by ``SpTabReportRetrieval.retrieve_report()``.
    """

    REPORT_CREATED = 1
    """
    Signifies that the tracker was updated with a report being created with no exceptions being raised. Another error could have been returned by the API.
    """

    DOCUMENTED_RETURNED = 2
    """
    Signifies that the tracker was updated with a document being returned (and that the status was updated to DONE).
    """

    UPDATED_STATUS = 3
    """
    Signifies that the tracker has been updated with a new status (other than DONE).
    """

    DONE_NOTHING = 4
    """
    Signifies that no change was made to the tracker.
    """

    EXCEPTION_OCCURRED = 5
    """
    Signifies that an error occurred in CREATE REPORT or GET REPORT requests and the tracker is updated with the associated exception information.
    """


class SpReportTracker:
    """
    This class serves as an interface for you to implement a tracker for the status of reports created for a particular type, marketplace, and date range. This will be used by `SpTabReportRetrieval`. Some examples are listed in the See Also section below.

    See Also
    --------
    DictTracker

    `SpTabReportRetrieval.retrieve_report()`
    """

    def init_report_tracking(self, report_type_name, marketplace, start_ds, end_ds, report_id, errors):
        """
        This method should update your tracking system when a new report has been created.  

        Parameters
        ----------
        report_type_name : str
            Name of the report being created. This will be identical to what you would pass into ``SpTabReportRetrieval.retrieve_report()``.
        marketplace : str
            Country code for the report being created. This will be identical to what you would pass into ``SpTabReportRetrieval.retrieve_report()``.
        start_ds : str
            yyyy-MM-dd string of start of date range. This will be identical to what you would pass into ``SpTabReportRetrieval.retrieve_report()``.
        end_ds : str
            yyyy-MM-dd string of end of date range. This will be identical to what you would pass into ``SpTabReportRetrieval.retrieve_report()``.
        report_id : str
            Report ID of report that is created in `SpTabReportRetrieval.retrieve_report()`.
        errors : str
            Errors from report creation that is created in `SpTabReportRetrieval.retrieve_report()`.

        Notes
        -----
        Its return value will be ignored. You can choose the criteria for report creation.
        """

        pass

    def is_report_created(self, report_type_name, marketplace, start_ds, end_ds):
        """
        This method should return whether or not a report has been created. This method will be closely related to `SpReportTracker.init_report_tracking()`.

        Parameters
        ----------
        report_type_name : str
            Name of the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        marketplace : str
            Country code for the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        start_ds : str
            yyyy-MM-dd string of start of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        end_ds : str
            yyyy-MM-dd string of end of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.

        Returns
        -------
        report_created : bool
            Whether or not the report has been created. 

        Notes
        -----
        The criteria that determines whether a report has been created is up to you.
        """

        pass

    def get_report_id(self, report_type_name, marketplace, start_ds, end_ds):
        """
        Get the report ID associated with the report created for a provided type, marketplace, and date range. This should be the report ID passed into `SpReportTracker.init_report_tracking()`.

        Parameters
        ----------
        report_type_name : str
            Name of the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        marketplace : str
            Country code for the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        start_ds : str
            yyyy-MM-dd string of start of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        end_ds : str
            yyyy-MM-dd string of end of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.

        Returns
        -------
        report_id : str
            The report ID associated with the provided report_type_name, marketplace, and date range.

        Notes
        -----
        If your implementation raises an exception, it will fall through.
        """

        pass

    def get_report_status(self, report_type_name, marketplace, start_ds, end_ds):
        """
        Get the status associated with the report created for a provided type, marketplace, and date range. This should be the status passed into `SpReportTracker.init_report_tracking()` or calls to `SpTabReportRetrieval.retrieve_report()`.

        Parameters
        ----------
        report_type_name : str
            Name of the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        marketplace : str
            Country code for the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        start_ds : str
            yyyy-MM-dd string of start of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        end_ds : str
            yyyy-MM-dd string of end of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.

        Returns
        -------
        status : str
            The status associated with the provided report_type_name, marketplace, and date range.

        Notes
        -----
        If your implementation raises an exception, it will fall through.
        """

        pass

    def update_report_status(self, report_type_name, marketplace, start_ds, end_ds, status, errors):
        """
        This method should report the status and errors for the report retrieval process for a provided report type, marketpalce, and date range.

        Parameters
        ----------
        report_type_name : str
            Name of the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        marketplace : str
            Country code for the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        start_ds : str
            yyyy-MM-dd string of start of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        end_ds : str
            yyyy-MM-dd string of end of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        status : str
            New status for the report associated with the provided type name, marketplace, and date range.
        errors : str
            New error information for the report associated with the provided type name, marketplace, and date range.

        Notes
        -----
        Its return value will be ignored.
        """

        pass

    def update_report_document_id(self, report_type_name, marketplace, start_ds, end_ds, doc_id):
        """
        This method should report the status and errors for the report retrieval process for a provided report type, marketpalce, and date range.

        Parameters
        ----------
        report_type_name : str
            Name of the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        marketplace : str
            Country code for the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        start_ds : str
            yyyy-MM-dd string of start of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        end_ds : str
            yyyy-MM-dd string of end of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        status : str
            The new document ID associated with the above parameters.

        Notes
        -----
        Its return value will be ignored.
        """

        pass

    def get_report_document_id(self, report_type_name, marketplace, start_ds, end_ds):
        """
        Get the document ID associated with the report created for a provided type, marketplace, and date range. This should be the document ID set in `SpTabReportRetrieval.retrieve_report()` via `SpReportTracker.update_report_document_id()`.

        Parameters
        ----------
        report_type_name : str
            Name of the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        marketplace : str
            Country code for the report being created. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        start_ds : str
            yyyy-MM-dd string of start of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.
        end_ds : str
            yyyy-MM-dd string of end of date range. This will be identical to what you would pass into `SpTabReportRetrieval.retrieve_report()`.

        Returns
        -------
        doc_id : str
            The document ID associated with the provided report_type_name, marketplace, and date range. If there is no document ID for the provided parameters, return None.

        Notes
        -----
        If your implementation raises an exception, it will fall through.
        """

        pass


class SpTabReportRetrieval:
    """
    This class is used for the retrieval of tab reports.

    Parameters
    ----------
    tracker : SpReportTracker
        Tracker for logging the creation of a report (and its ID), status and errors.
    credentials : dict
        Dictionary with necessary credentials for making report requests.
    report_type_name : str
        The name of the tab report type that will be retrieved using this object. Can leave this out if you wish to specify a particular types for each retrieval in `SpTabReportRetrieval.retrieve_report()`.

    Raises
    ------
    ValueError
        If `report_type_name` is not a valid tab report type. If `marketplace` is not a valid marketplace.
    TypeError 
        If `tracker` is not a `SpReportTracker`.

    References
    ----------
    [Reports class from saleweaver's Python sp-api wrapper](https://github.com/saleweaver/python-amazon-sp-api/blob/master/sp_api/api/reports/reports.py)
    """

    # Statuses that signify waiting for a report
    __WAITING_STATUS = {None, 'IN_QUEUE', 'IN_PROGRESS'}

    # Represents 3 types of possible requests sent by retrieve_report()
    class __RequestType(Enum):
        CREATE_REPORT = 1
        GET_REPORT = 2
        GET_REPORT_DOC = 3

    def __init__(self, tracker, credentials=None, report_type_name=None):

        if report_type_name not in TAB_REPORT_TYPES:
            raise ValueError('Invalid Tab report type name %s' %
                             (report_type_name))
        if not isinstance(tracker, SpReportTracker):
            raise TypeError(
                'Tracker must be an instance of SpReportTracker. Got %s.' % (type(tracker)))
        self.__creds = credentials

        # will initialize the report later, seems like you can't have multiple instances of a Reports
        # objects with the same credentials, so we only set this using self.__creds/credentials in 
        # retrieve_report()
        self.__rep = None

        self.__tracker = tracker
        self.__type_name = report_type_name
        self.custom_mode()

    def get_report_type_name(self):
        """
        Gets the report type name specified at object construction. 

        Returns
        -------
        The value you passed into `report_type_name` at object construction (or None if you didn't provide one).
        """

        return self.__type_name

    def bulk_mode(self):
        """
        Puts the retrieval object in bulk retrieval mode. Rate limit checks will be performed in calls to `SpTabReportRetrieval.retrieve_report()`. This is useful for performing bulk operations with repeated requests of varying types. CREATE REPORT requests can be done once per minute in bursts of 15. GET REPORT requests can be done twice per second in bursts of 15. GET REPORT DOCUMENT requests can be done once per minute in bursts of 15.

        Warnings
        --------
        If you want to take the object out of bulk mode, you must run `SpTabReportRetrival.custom_mode()`. Otherwise, it will stay in bulk mode.

        References
        ----------
        [saleweaver Python sp-api](https://github.com/saleweaver/python-amazon-sp-api/blob/master/sp_api/api/reports/reports.py)

        See Also
        --------
        SpTabReportRetrieval.custom_mode()
        """

        self.__bulk = True
        self.__report_create_left = 15
        self.__get_report_left = 15
        self.__get_report_doc_left = 15
        self.__t_last_report_create = -1
        self.__t_last_get_report = -1
        self.__t_last_get_report_doc = -1

    def custom_mode(self):
        """
        Puts the retrieval object in custom retrieval mode. There will be no rate limit checks in calls to `SpTabReportRetrieval.retrieve_report()`. Useful for testing reports, performing a few specific calls. 
        """

        self.__bulk = False

    def retrieve_report(self, marketplace, start_ds, end_ds, credentials = None, report_type_name=None, **output_kwargs):
        """
        Retrieve report for marketplace between start and end dates. This method behaves in the following manner:
        1. If no information has been tracked for the provided report_type_name, marketplace, and date range then a report is created.
        2. If the object's tracker has an entry for the provided marketplace, date range, and report_type_name has a status of None, IN_QUEUE, or IN_PROGRESS, then the report is queried in the API with its ID. If its status is now DONE, then the document will also be retrieved and returned through `SpTabReportRetrieval.output_report_doc()`. Otherwise, the status will just be updated.
        3. If the object's tracker has a document_id for the provided report_type_name, marketplace, and date range then the document will be retrieved and returned through `SpTabReportRetrieval.output_report_doc()`.
        4. If the object's tracker has a report that has a status of something other than None, IN_QUEUE, or IN_PROGRESS, then nothing happens.

        Parameters
        ----------
        marketplace : str
            Amazon marketplace country code
        start_ds : str
            yyyy-MM-dd string representing start date
        end_ds : str
            yyyy-MM-dd string representing end date
        credentials: dict
            TODO
        report_type_name : str
            Name of the report type for retrieval (default: None). If not provided, defaults to `report_type_name` specified in object creation. If `report_type_name` specified in object creation, this will be used instead.
        output_kwargs : keyword arguments
            Any additional parameters that `output_report_doc()` needs. These will be passed directly to it. 

        Returns
        -------
        status : SpReportTrackingStatus
            What change was made to the associated tracker by this retrieval.
        out : object
            None if a report was created or nothing was done in the retrieval. The new status if the retrieval resulted in status being updated (to something other than DONE). The output of `SpTabReportRetrieval.output_report_doc()` if a document was generated and returned (in the event status became DONE).

        Raises
        ------
        RuntimeError
            If no `report_type_name` has been provided either in object construction or in arguments. 
            If a retrieval tries to be made to get more information on a report that has been marked as created in the tracker, but has no report ID.
        ValueError
            If `report_type_name` is not a valid tab report type. 
            If `marketplace` is not a valid marketplace.

        Notes
        -----
        The general code for performing bulk retrievals for a specific report type can be done as follows:
        1. Iterate over each specification s
        2. If a report hasn't been created for s or s has no document ID, make retrieval to see if report processing can make progress. If a report has been created and has a document ID, we could be done (need to check rest of specifications).
        3. If retrieval status was `SpReportTrackingStatus.DONE_NOTHING`, then report processing has been moved to FATAL/CANCELLED. If this is the case, we could be done. Other statuses indicate we can keep going.
        There are other ways to implement this same strategy, including detecting an exception occuring / document generation in step 3 to avoid more future iterations.

        See Also
        --------
        TAB_REPORT_TYPES

        NA_MARKETPLACE_COUNTRY_CODESET

        EUR_MARKETPLACE_COUNTRY_CODESET

        SpReportTrackingStatus

        SpTabReportRetrieval.output_report_doc()

        SpReportTracker
        """

        # if report type name not specified here, use one from object initialization, if none was provided at initialization, raise error
        if report_type_name is None:
            if self.__type_name is None:
                raise RuntimeError(
                    'Must specify report type name in either object construction or via report_type_name. Both cannot be None.')
            report_type_name = self.__type_name

        if credentials is None:
            if self.__creds is None:
                raise RuntimeError(
                    'Must specify credentials in either object construction or via credentials. Both cannot be None.')
            self.__rep = Reports(credentials=self.__creds)
        else:
            self.__rep = Reports(credentials=credentials)

        status = SpReportTrackingStatus.DONE_NOTHING
        out = None

        # if not report created of this type, marketplace, and date range - create one
        if not self.__tracker.is_report_created(report_type_name, marketplace, start_ds, end_ds):
            status = self.__create_report(
                report_type_name, marketplace, start_ds, end_ds)
        # if report is waiting to be processed - process it with __update_report_status
        elif self.__tracker.get_report_status(report_type_name, marketplace, start_ds, end_ds) in SpTabReportRetrieval.__WAITING_STATUS:
            status, out = self.__update_report_status(
                report_type_name, marketplace, start_ds, end_ds, **output_kwargs)
        # if report was already done, just get its document ID from the tracker and use it to get the document
        elif self.__tracker.get_report_document_id(report_type_name, marketplace, start_ds, end_ds) is not None:
            status = SpReportTrackingStatus.DOCUMENTED_RETURNED
            doc_id = self.__tracker.get_report_document_id(
                report_type_name, marketplace, start_ds, end_ds)
            out = self.__get_document_df(
                report_type_name, marketplace, start_ds, end_ds, doc_id, **output_kwargs)
        # else : report has reached FATAL/CANCELLED status - do nothing

        return status, out

    def output_report_doc(self, marketplace, start_ds, end_ds, report_type_name, df, **kwargs):
        """
        This method takes the output of the report generated for a marketplace, date range, and marketplace and returns it. However, you should write subclasses that inherit `SpTabReportRetrieval` and override this method.

        Parameters
        ----------
        marketplace : str
            Amazon marketplace country code
        start_ds : str
            yyyy-MM-dd string representing start date
        end_ds : str
            yyyy-MM-dd string representing end date
        report_type_name : str
            Name of the report type for retrieval (default: None). If not provided, defaults to `report_type_name` specified in object creation. If `report_type_name` specified in object creation, this will be used instead.
        df : pd.DataFrame
            DataFrame holding the output of the generated document corresponding to the previous four parameters. In overridden versions of this method, you can write this to a file or database.
        kwargs : keyword arguments
            This will hold the keyword arguments passed as `**output_kwargs` to `SpTabReportRetrieval.retrieve_report()`. Use this as a way to retrieve user sent information needed in sending the output to its desired destination.

        Returns
        -------
        df : pd.DataFrame
            Outputs input DataFrame. However, in overridden implementations, you should return whatever works best. The output will be passed through `SpTabReportRetrieval.retrieve_report()` as `out.`

        See Also
        --------
        `SpTabReportRetrieval.retrieve_report()`
        """

        return df

    ######################################### PRIVATE METHODS #############################################

    # Usage: create a report for type, marketplace, and date range
    def __create_report(self, report_type_name, marketplace, start_ds, end_ds):
        if report_type_name not in TAB_REPORT_TYPES:
            raise ValueError('Invalid Tab report type name %s' %
                             (report_type_name))
        if marketplace not in NA_MARKETPLACE_COUNTRY_CODESET and marketplace not in EUR_MARKETPLACE_COUNTRY_CODESET:
            raise ValueError('Invalid marketplace %s' % (marketplace))

        # get start, end dates in ISO 8601
        start_iso = datetime.strptime(start_ds, '%Y-%m-%d').isoformat()
        end_iso = datetime.strptime(end_ds, '%Y-%m-%d').isoformat()

        # get marketplace ID from code
        mplaceid = eval('Marketplaces.%s.marketplace_id' % (marketplace))

        try:
            # wait so it's safe to sent CREATE REPORT request if in bulk mode, then make the request and get the response
            self.__wait(SpTabReportRetrieval.__RequestType.CREATE_REPORT)
            create_res = self.__rep.create_report(
                reportType=TAB_REPORT_TYPES[report_type_name], dataStartTime=start_iso, dataEndTime=end_iso, marketplaceIds=[mplaceid])

            # initialize tracking for the report of specified type, marketplace and date range with the id and errors
            report_id = create_res.payload['reportId']
            self.__tracker.init_report_tracking(
                report_type_name, marketplace, start_ds, end_ds, report_id, create_res.errors)
            return SpReportTrackingStatus.REPORT_CREATED

        except SellingApiException as e:
            # pass exception information onto tracker so user can investigate
            self.__tracker.init_report_tracking(
                report_type_name, marketplace, start_ds, end_ds, None, "[%s] %s" % (type(e), str(e)))
            return SpReportTrackingStatus.EXCEPTION_OCCURRED

    # Usage: get document output from output_report_doc()
    def __get_document_df(self, report_type_name, marketplace, start_ds, end_ds, doc_id, **output_kwargs):

        # wait for GET-REPORT-DOCUMENT request if in bulk mode
        self.__wait(SpTabReportRetrieval.__RequestType.GET_REPORT_DOC)

        # get document from API and load it into DataFrame
        doc = self.__rep.get_report_document(doc_id, decrypt=True)
        doc = doc.payload['document']
        df = pd.read_csv(StringIO(doc), sep='\t')

        # pass in dataframe; marketplace, date range, and type of report; and lastly the output keyword arguments passed from retrieve_report()
        return self.output_report_doc(marketplace, start_ds, end_ds, report_type_name, df, **output_kwargs)

    # Usage: update report status for type, marketplace, and date range for a created report
    def __update_report_status(self, report_type_name, marketplace, start_ds, end_ds, **output_kwargs):
        try:
            # wait for GET-REPORT request if in bulk mode
            self.__wait(SpTabReportRetrieval.__RequestType.GET_REPORT)

            # get report id from tracker and make sure its not None
            report_id = self.__tracker.get_report_id(
                report_type_name, marketplace, start_ds, end_ds)
            if not report_id:
                raise RuntimeError('Cannot retrieve status/document without a report ID. Check errors for (%s,%s,%s,%s) in your tracker.' %
                                   (report_type_name, marketplace, start_ds, end_ds))

            # make request and collect status of report
            res = self.__rep.get_report(report_id)
            status = res.payload['processingStatus']

            # update tracker with status and any errors
            self.__tracker.update_report_status(
                report_type_name, marketplace, start_ds, end_ds, status, res.errors)

            # if report is now done, get the report document using helper method __get_document_df() which uses subclass output_report_doc() (polymorphism)
            if status == 'DONE':
                doc_id = res.payload['reportDocumentId']
                self.__tracker.update_report_document_id(
                    report_type_name, marketplace, start_ds, end_ds, doc_id)
                out = self.__get_document_df(
                    report_type_name, marketplace, start_ds, end_ds, doc_id, **output_kwargs)
                return SpReportTrackingStatus.DOCUMENTED_RETURNED, out
            else:  # just a status update, return the new status to caller
                return SpReportTrackingStatus.UPDATED_STATUS, status
        except SellingApiException as e:
            # error occurred in retrieval process, return last status saved in tracker - if error occurred in status update, then this will be the old one.
            # if error occurred in document retrieval, this will be status last returned from api
            status = self.__tracker.get_report_status(
                report_type_name, marketplace, start_ds, end_ds)
            self.__tracker.update_report_status(
                report_type_name, marketplace, start_ds, end_ds, status, type(e).__name__ + str(e))
            return SpReportTrackingStatus.EXCEPTION_OCCURRED, status

    # Usage: has retrieval object wait for a specified request type if in bulk mode
    def __wait(self, req_type):
        if not self.__bulk:
            return
        curr = time.time()
        if req_type == SpTabReportRetrieval.__RequestType.CREATE_REPORT:
            # if CREATE-REPORT request has been done before, make sure that enough time has passed since then
            if self.__t_last_report_create > 0:
                diff = curr - self.__t_last_report_create
                # must wait 1 min between CREATE-REPORT requests
                if diff < 60:
                    time.sleep(60 - diff)
            # update last CREATE-REPORT time, decrement number of CREATE-REPORT requests left in burst
            self.__t_last_report_create = curr
            self.__report_create_left -= 1

            # resetting count after waiting a minute
            if self.__report_create_left == 0:
                time.sleep(60)
                self.__report_create_left = 15
        elif req_type == SpTabReportRetrieval.__RequestType.GET_REPORT:
            # if GET-REPORT request has been done before, make sure that enough time has passed since then
            if self.__t_last_get_report > 0:
                diff = curr - self.__t_last_get_report
                # must wait 0.5s between GET-REPORT requests
                if diff < 0.5:
                    time.sleep(0.5 - diff)
            # update last GET-REPORT time, decrement number of GET-REPORT requests left in burst
            self.__t_last_get_report = curr
            self.__get_report_left -= 1

            # resetting count after waiting a minute
            if self.__get_report_left == 0:
                time.sleep(60)
                self.__get_report_left = 15
        else:
            # if GET-REPORT-DOCUMENT request has been done before, make sure that enough time has passed since then
            if self.__t_last_get_report_doc > 0:
                diff = curr - self.__t_last_get_report_doc
                # must wait 1 min between GET-REPORT-DOCUMENT requests
                if diff < 60:
                    time.sleep(60 - diff)
            # update last GET-REPORT-DOCUMENT time, decrement number of GET-REPORT-DOCUMENT requests left in burst
            self.__t_last_get_report_doc = curr
            self.__get_report_doc_left -= 1

            # resetting count after waiting a minute
            if self.__get_report_doc_left == 0:
                time.sleep(60)
                self.__get_report_doc_left = 15


# sample trackers

class DictTracker(SpReportTracker):
    """
    Class that tracks report retrieval in a dictionary. It also provides the user with the ability to load/save dictionary information from/to a file for more permanent storage.

    Parameters
    ----------
    fp : str
        File path to load tracker from. Default: None -> tracker starts empty.
    """

    def __init__(self, fp=None):
        self.d = dict()
        if fp:
            f = open(fp, 'rb')
            self.d = pkl.load(f)

    def init_report_tracking(self, report_type_name, marketplace, start_ds, end_ds, report_id, errors):
        """
        See documentation for `SpReportTracker.init_report_tracking()`.
        """

        self.d[(report_type_name, marketplace, start_ds, end_ds)] = {
            'ReportId': report_id, 'Errors': errors, 'Status': None, 'DocumentId': None}

    def is_report_created(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.is_report_created()`.
        """

        return (report_type_name, marketplace, start_ds, end_ds) in self.d

    def get_report_id(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.get_report_id()`.
        """

        return self.d[(report_type_name, marketplace, start_ds, end_ds)]['ReportId']

    def get_report_status(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.get_report_status()`.
        """

        return self.d[(report_type_name, marketplace, start_ds, end_ds)]['Status']

    def update_report_status(self, report_type_name, marketplace, start_ds, end_ds, status, errors):
        """
        See documentation for `SpReportTracker.update_report_status()`.
        """

        self.d[(report_type_name, marketplace,
                start_ds, end_ds)]['Status'] = status
        self.d[(report_type_name, marketplace,
                start_ds, end_ds)]['Errors'] = errors

    def update_report_document_id(self, report_type_name, marketplace, start_ds, end_ds, doc_id):
        """
        See documentation for `SpReportTracker.update_report_document_id()`.
        """

        self.d[(report_type_name, marketplace, start_ds, end_ds)
               ]['DocumentId'] = doc_id

    def get_report_document_id(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.get_report_document_id()`.
        """

        return self.d[(report_type_name, marketplace, start_ds, end_ds)]['DocumentId']

    def __str__(self):
        """
        Returns a string representation of the tracked information for user convenience. This is an example of additional methods you could add to your own tracker.

        Returns
        -------
        out : str
            String with tracked information for each (report_type_name, marketplace, start_ds, date range) entry on separate lines.
        """

        out = "\n".join(["%s:%s" % (str(k), str(self.d[k])) for k in self.d])
        return out

    def save(self, fp):
        """
        Saves the state in the tracker to a pickle file. This is another possible method one could provide in a subclass.

        Parameters
        ----------
        fp : str
            File path to write to.
        """

        f = open(fp, 'wb')
        pkl.dump(self.d, f)


class DBTracker(SpReportTracker):
    """
    Class that tracks report retrieval in a MySQL database. 

    Parameters
    ----------
    conn : MySQL_DB_Connection
        Connection to write to
    schema_name : str
        Name of schema where tracking will occur

    Raises
    ------
    ValueError
        If the columns of the schema instance on `conn` with name `schema_name` are not a subset of `DBTracker.REQUIRED_COLUMNS`.
    """

    REQUIRED_COLUMNS = {'report_type_name', 'marketplace', 'start_ds',
                        'end_ds', 'report_id', 'status', 'errors', 'document_id'}
    """
    Columns required in schema on `conn` with name `schema_name` for tracking.
    """

    def __init__(self, conn, schema_name):
        self.__conn = conn
        self.__schema = self.__conn.get_schema(schema_name)
        if not DBTracker.REQUIRED_COLUMNS.issubset(set(self.__schema.get_column_names())):
            raise ValueError('%s must have all of %s' % (
                schema_name, ', '.join(DBTracker.REQUIRED_COLUMNS)))

    def init_report_tracking(self, report_type_name, marketplace, start_ds, end_ds, report_id, errors):
        """
        See documentation for `SpReportTracker.init_report_tracking()`.
        """

        self.__conn.insert(self.__schema, [
                           report_type_name, marketplace, start_ds, end_ds, report_id, None, errors, None])

    def is_report_created(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.is_report_created()`.
        """

        return not self.__conn.key_get(self.__schema, (report_type_name, marketplace, start_ds, end_ds)).empty

    def get_report_id(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.get_report_id()`.
        """

        return self.__conn.key_get(self.__schema, (report_type_name, marketplace, start_ds, end_ds)).loc[0, 'report_id']

    def get_report_status(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.get_report_status()`.
        """

        return self.__conn.key_get(self.__schema, (report_type_name, marketplace, start_ds, end_ds)).loc[0, 'status']

    def update_report_status(self, report_type_name, marketplace, start_ds, end_ds, status, errors):
        """
        See documentation for `SpReportTracker.update_report_status()`.
        """

        existing = self.__conn.key_get(
            self.__schema, (report_type_name, marketplace, start_ds, end_ds)).loc[0, :]
        self.__conn.insert(self.__schema, [report_type_name, marketplace, start_ds,
                                           end_ds, existing['report_id'], status, errors, existing['document_id']], overwrite=True)

    def update_report_document_id(self, report_type_name, marketplace, start_ds, end_ds, doc_id):
        """
        See documentation for `SpReportTracker.update_report_document_id()`.
        """

        existing = self.__conn.key_get(
            self.__schema, (report_type_name, marketplace, start_ds, end_ds)).loc[0, :]
        self.__conn.insert(self.__schema, [report_type_name, marketplace, start_ds,
                                           end_ds, existing['report_id'], existing['status'], existing['errors'], doc_id], overwrite=True)

    def get_report_document_id(self, report_type_name, marketplace, start_ds, end_ds):
        """
        See documentation for `SpReportTracker.get_report_document_id()`.
        """

        return self.__conn.key_get(self.__schema, (report_type_name, marketplace, start_ds, end_ds)).loc[0, 'document_id']

# sample subclasses


class TypeSampleRetrieval(SpTabReportRetrieval):

    def __init__(self, credentials, tracker):
        super().__init__(credentials, tracker)

    def output_report_doc(self, marketplace, start_ds, end_ds, report_type_name, df, **kwargs):
        df.to_excel('%s.xlsx' % (report_type_name), index=False)
        return df


class InventoryToSchemaRetrieval(SpTabReportRetrieval):

    def __init__(self, credentials, tracker, db_conn, schema_name):
        super().__init__(credentials, tracker, 'FBA Amazon Fulfilled Inventory Report')
        self.__schema = db_conn.get_schema(schema_name)
        self.__conn = db_conn

    def output_report_doc(self, marketplace, start_ds, end_ds, report_type_name, df, **kwargs):
        last_row = len(df.index) - 1
        if isinstance(df.iat[last_row, 5], str):
            df.iat[last_row, 5] = re.sub('[^0-9]', '', df.iat[last_row, 5])
            df['Quantity Available'] = df['Quantity Available'].astype('int64')
        df['date'] = start_ds
        out = df.groupby(['date', 'asin'])['Quantity Available'].sum()
        out = out.to_frame(name='quantity').reset_index()
        out = out[self.__schema.get_column_names()]
        self.__conn.insert(self.__schema, out, status_check=False)
        return len(out.index)
