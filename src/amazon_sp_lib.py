"""
This file holds a wrapper class for retrieving orders with Amazon's sp-api.
"""
# Chami Lamelas
# Last updated 3/5/2021
from datetime import datetime, timedelta
from sp_api.api import Orders
from sp_api.base import SellingApiRequestThrottledException, SellingApiServerException
from sp_api.base import Client, Marketplaces
from tqdm import tqdm
import dateutil.parser as dtparser
import pandas as pd
import json
import time
import pytz

class SP_Orders_Retrieval:
    """
    This class is to be used for retrieving orders for a particular day and marketplace. 

    Parameters
    ----------
    NA_creds : dict
        The credentials needed for countries in North America to be used by sp_api.api.Orders
    EUR_creds : dict
        The credentials needed for countries in Europe to be used by sp_api.api.Orders
    rates : dict
        The exchange rates for each marketplace and day that will be used to convert currencies to USD in the order payload. The dictionary should map (day,currency) -> float.

    Other Parameters
    ----------------
    **kwargs
        Keyword arguments to specify request processing settings. `timezone` is used to specify that all orders retrieval will be on intervals in one timezone. By default, the orders for a day retrieved in `SP_Orders_Retrieval.retrieve_orders` will be retrieved on an interval specified by `SP_Orders_Retrieval.REPORT_TIMEZONES`. `request_pause_time` is used to specify the amount of time (seconds) to wait between requests. By default, it is 0.2 seconds as specified in the sp-api rate limits discussion. `request_burst_size` is used to specify the number of requests that can be sent in a burst at the rate specified by `request_pause_time`. By default, it is 15 as specifed in sp-api rate limits. `burst_pause_time` is the amount of time (seconds) to wait after a burst. By default, it's 30 s. 

    Raises
    ------
    ValueError
        If you provide a `timezone` that is not a valid `pytz` timezone

    References
    ----------
    [Developer Guide](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/developer-guide/SellingPartnerApiDeveloperGuide.md)

    [Supported Amazon Currencies](https://www.amazon.com/gp/help/customer/display.html/ref=s9_acss_bw_cg_ACCBLand_3a1_w?nodeId=201894850&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-2&pf_rd_r=FYEXRCKKHT3KFPERXRD0&pf_rd_t=101&pf_rd_p=82bf0150-d0c9-411f-982f-b0c577129b1c&pf_rd_i=388305011)

    [Rate Limits](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/usage-plans-rate-limits/Usage-Plans-and-Rate-Limits.md)

    See Also
    --------
    SP_Orders_Retrieval.retrieve_orders
    SP_Orders_Retrieval.REPORT_TIMEZONES
    """

    NA_CODES = ['US','CA','BR','MX']
    """
    Country codes for countries in North America supported in sp-api
    """

    EUR_CODES = ['ES','GB','FR','NL','DE','IT','SE','TR','AE','IN','UK']
    """
    Country codes for countries in Europe supported in sp-api (note: UK is alias for GB as in sp-api)

    References
    ----------
    [sp-api marketplaces.py](https://github.com/saleweaver/python-amazon-sp-api/blob/master/sp_api/base/marketplaces.py)
    """

    ORDERS_COLUMNS = ['amazon_order_id', 'purchase_date', 'last_update_date', 'order_status', 'fulfillment_channel', 'sales_channel', 'ship_service_level', 'order_total_usd', 'number_of_items_shipped', 'number_of_items_unshipped', 'is_replacement_order', 'marketplace_id', 'shipment_service_level_category', 'earliest_ship_date', 'latest_ship_date', 'is_prime', 'is_global_express_enabled', 'is_premium_order']
    """
    Names of the columns in the orders DataFrame returned by `SP_Orders_Retrieval.retrieve_orders`

    See Also
    --------
    SP_Orders_Retrieval.retrieve_orders
    """

    ITEMS_COLUMNS = ['amazon_order_id', 'order_item_id', 'asin', 'is_gift', 'item_price', 'item_tax', 'promotion_discount', 'promotion_discount_tax', 'quantity_ordered', 'quantity_shipped', 'seller_sku', 'shipping_price', 'shipping_tax', 'shipping_discount']
    """
    Names of the columns in the items DataFrame returned by `SP_Orders_Retrieval.retrieve_orders`

    See Also
    --------
    SP_Orders_Retrieval.retrieve_orders
    """

    REPORT_TIMEZONES = {
        'BR' : 'America/Sao_Paulo',
        'CA' : 'America/Los_Angeles',
        'MX' : 'America/Los_Angeles',
        'US' : 'America/Los_Angeles',
        'ES' : 'Europe/Paris',
        'GB' : 'Europe/London',
        'FR' : 'Europe/Paris',
        'NL' : 'Europe/Paris',
        'DE' : 'Europe/Paris',
        'IT' : 'Europe/Paris',
        'SE' : 'Europe/Paris',
        'TR' : 'Europe/Paris',
        'AE' : 'Europe/Paris',
        'IN' : 'Asia/Kolkata',
        'UK' : 'Europe/London',
        'AU' : 'Australia/Sydney',
        'SG' : 'Asia/Tokyo',
        'JP' : 'Asia/Tokyo'
    }
    """
    Timezones used by reports in different countries. Timezones are selected based on the FAQ for all the marketplaces that are listed. For those that are not listed, closest timezone from FAQ is selected.

    References
    ----------
    [Report Time Zones FAQ](https://developer.amazon.com/docs/reports-promo/reporting-FAQ.html)
    [pytz Time Zone List](https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568)
    """

    def __init__(self, NA_creds, EUR_creds, rates, **kwargs):

        # Map North America country codes to use North America credentials (used in __construct_orders)
        self.__country_code_map = {e:NA_creds for e in SP_Orders_Retrieval.NA_CODES}
        # Add to map, Europe country codes to Europe credentials (used in __construct_orders)
        self.__country_code_map.update({e:EUR_creds for e in SP_Orders_Retrieval.EUR_CODES})
        # Save rates (used in __convert_to_usd)
        self.__rates = rates
        if 'timezone' in kwargs and kwargs['timezone'] not in pytz.all_timezones_set:
            raise ValueError('%s is not a valid pytz timezone' % (kwargs['timezone']))

        # Save timezone (if None, REPORT_TIMEZONES will be used in its place)
        self.__timezone = kwargs['timezone'] if 'timezone' in kwargs else None

        # Save request processing settings
        self.__request_pause_time = kwargs['request_pause_time'] if 'request_pause_time' in kwargs else 0.2
        self.__request_burst_size = kwargs['request_burst_size'] if 'request_burst_size' in kwargs else 15
        self.__burst_pause_time = kwargs['burst_pause_time'] if 'burst_pause_time' in kwargs else 30

        # number of requests made in objects life time in current burst
        self.__req_count = 0

    # Construct Orders object for a marketplace specified by a VALID country code
    def __construct_orders(self, country_code):

        # retrieve credentials for this country code 
        creds = self.__country_code_map.get(country_code)

        if not creds:
            raise KeyError('%s is an invalid country code.' % (country_code))

        # construct Orders object with credentials and marketplace built from country_code
        return Orders(credentials=creds, marketplace=eval("Marketplaces." + country_code))

    # Construct request date interval from date string
    def __construct_interval(self, marketplace_code, date_str):
        after_naive=datetime.strptime(date_str, '%Y-%m-%d')
        tz_obj = pytz.timezone(self.__timezone if self.__timezone else SP_Orders_Retrieval.REPORT_TIMEZONES[marketplace_code])    
        after_local = tz_obj.localize(after_naive)
        after_utc = after_local.astimezone(pytz.utc)
        after_str = after_utc.isoformat()[:-6] + 'Z'
        before_str = (after_utc + timedelta(days=1)).isoformat()[:-6] + 'Z'
        return after_str, before_str

    # Convert datetime string to datetime 
    @staticmethod
    def __build_datetime(date_str):
        return dtparser.parse(date_str) if date_str else None

    # Convert price amount to USD for particular day
    def __convert_to_usd(self, day_str, price):
        if not price:
            return None

        # retrieve conversion rate for day and Amazon currency code
        rate = self.__rates[(day_str, price['CurrencyCode'])]
        return rate*float(price['Amount'])

    # Converts a string to boolean, bool_str.lower() == 'true' => True, else => False
    @staticmethod
    def __str_to_bool(bool_str):
        if not bool_str:
            return None
        return bool_str.lower() == 'true'

    def __make_request(self, o, request_type, **kwargs):
        # if request count has reached burst size, pause and reset request counter
        if self.__req_count == self.__request_burst_size:
            time.sleep(self.__burst_pause_time)
            self.__req_count = 0

        # make request after appropriately pausing, return payload
        self.__req_count += 1
        time.sleep(self.__request_pause_time)
        if request_type == 'orders':
            # 2 types of order requests: with created after, before range or next token
            res = o.get_orders(NextToken=kwargs['next_token']) if 'next_token' in kwargs else o.get_orders(CreatedAfter=kwargs['after'],CreatedBefore=kwargs['before'])
            return res.payload
        else:
            res = o.get_order_items(kwargs['order_id'])
            return res.payload

    def __make_order(self, order_dict, day_str):
        # Build order, converting dates, bools, and prices to USD as neccessary 
        order = [
            order_dict['AmazonOrderId'],
            SP_Orders_Retrieval.__build_datetime(order_dict.get('PurchaseDate')),
            SP_Orders_Retrieval.__build_datetime(order_dict.get('LastUpdateDate')),
            order_dict.get('OrderStatus'),
            order_dict.get('FulfillmentChannel'),
            order_dict.get('SalesChannel'),
            order_dict.get('ShipServiceLevel'),
            self.__convert_to_usd(day_str, order_dict.get('OrderTotal')),
            order_dict.get('NumberOfItemsShipped'),
            order_dict.get('NumberOfItemsUnshipped'),
            SP_Orders_Retrieval.__str_to_bool(order_dict.get('IsReplacementOrder')),
            order_dict.get('MarketplaceId'),
            order_dict.get('ShipmentServiceLevelCategory'),
            SP_Orders_Retrieval.__build_datetime(order_dict.get('EarliestShipDate')),
            SP_Orders_Retrieval.__build_datetime(order_dict.get('LatestShipDate')),
            order_dict.get('IsPrime'),
            order_dict.get('IsGlobalExpressEnabled'),
            order_dict.get('IsPremiumOrder')
        ]
        return order

    def __make_item(self, o, order_id, item_dict, day_str):
        # Build item, converting bools and prices to USD as neccessary 
        item = [
            order_id,
            item_dict['OrderItemId'],
            item_dict.get('ASIN'),
            SP_Orders_Retrieval.__str_to_bool(item_dict.get('IsGift')),
            self.__convert_to_usd(day_str, item_dict.get('ItemPrice')),
            self.__convert_to_usd(day_str, item_dict.get('ItemTax')),
            self.__convert_to_usd(day_str, item_dict.get('PromotionDiscount')),
            self.__convert_to_usd(day_str, item_dict.get('PromotionDiscountTax')),
            item_dict.get('QuantityOrdered'),
            item_dict.get('QuantityShipped'),
            item_dict.get('SellerSKU'),
            self.__convert_to_usd(day_str, item_dict.get('ShippingPrice')),
            self.__convert_to_usd(day_str, item_dict.get('ShippingTax')),
            self.__convert_to_usd(day_str, item_dict.get('ShippingDiscount'))
        ]
        return item
    
    # Processes payload using Orders object 'o' on date 'day_str'. 'batch' is the payload number (see retrieve_orders)
    def __process_payload(self, o, day_str, batch_num, batch_payload, debug=None):

        # lists of orders, items
        order_batch = []
        item_batch = []

        # use tqdm if user wants to see progress bars, else iterate over payload
        itr = tqdm(batch_payload, desc='Progress of orders on batch %d' % (batch_num)) if debug == "tqdm" else batch_payload

        ti = time.time()
        # Iterating over order dictionaries in payload
        for order_dict in itr:
            # construct new order list and add it to batch
            order_batch.append(self.__make_order(order_dict, day_str)) 
            # request items associated with order
            items_list = self.__make_request(o, 'items', order_id=order_dict['AmazonOrderId'])
            # construct new item list for each and add it to batch
            for item_dict in items_list['OrderItems']:
                item_batch.append(self.__make_item(o, order_dict['AmazonOrderId'], item_dict, day_str))

        if debug == "print":
            print("Processed payload with %d orders and %d items in %.2f s." % (len(order_batch), len(item_batch), (time.time() - ti)))
        return order_batch, item_batch

    def retrieve_orders(self, country_code, date_str, debug=None):
        """
        Retrieves the orders for a particular day and country code.

        Parameters
        ----------
        country_code : str
            Country code to retrieve orders from
        date_str : str
            yyyy-MM-dd date string
        debug : str
            Controls debugging information (default: `None` - no information is displayed). Other options: "print" - number of orders, items in each batch/payload printed, if there are API exceptions they are printed as well and "tqdm" - no printing is done, but a progress bar is shown over batch/payload 

        Returns
        -------
        orders_df : pd.DataFrame
            Pandas DataFrame with orders data specified in `SP_Orders_Retrieval.ORDERS_COLUMNS`
        items_df : pd.DataFrame
            Pandas DataFrame with items data specified in `SP_Orders_Retrieval.ITEMS_COLUMNS`

        Raises
        ------
        KeyError
            If `country_code` is not a valid sp-api marketplace
        SellingApiExecution
            If sp-api GET requests could not be processed
        SellingApiRequestThrottledException
            If sp-api requests are being throttled
        SellingApiServerException
            If an sp-api internal error occurred
        
        Warnings
        --------
        `orders_df` and `items_df` may contain `None` and `np.nan` values

        See Also
        --------
        SP_Orders_Retrieval.ORDERS_COLUMNS
        SP_Orders_Retrieval.ITEMS_COLUMNS

        Notes
        -----
        Column `order_item_id` is unique to an order (specified by column `amazon_order_id`).
        Columns of `datetime` are all UTC/GMT time as that is the way in which they are returned from Amazon.

        References
        ----------
        [Developer Guide](https://github.com/amzn/selling-partner-api-docs/blob/main/guides/developer-guide/SellingPartnerApiDeveloperGuide.md)

        [ListOrderItems Docs](https://docs.developer.amazonservices.com/en_US/orders-2013-09-01/Orders_Datatypes.html#OrderItem)

        [OrderItemID Discussion](https://sellercentral.amazon.com/forums/t/is-orderitemid-value-unique-to-the-order-or-the-item/29368)
        """

        # retrieve orders object based on country_code
        o = self.__construct_orders(country_code)

        # construct request interval (current day)
        after, before = self.__construct_interval(country_code, date_str)

        # Build up orders and items information as lists from each batch
        # Convert to DataFrame at the end for runtime + space efficiency
        orders_data = []
        items_data = []

        # make request, any generated exceptions are raised to caller
        res = self.__make_request(o, 'orders', after=after, before=before)

        # collect batches till last payload starting from batch=1
        last_batch = False
        batch_num = 1
        while not last_batch:

            # retrieve token for next batch
            token = res.get('NextToken')

            # get orders, items for this batch
            order_batch, item_batch = self.__process_payload(o, date_str, batch_num, res['Orders'], debug)

            # add batch orders, items to total
            orders_data.extend(order_batch)
            items_data.extend(item_batch)
            batch_num += 1

            if token:
                res = self.__make_request(o, 'orders', next_token=token)
            else: # no token for another batch, we're done
                last_batch = True

        # build dataframes out of lists with specified columns
        orders_df = pd.DataFrame(data=orders_data, columns=SP_Orders_Retrieval.ORDERS_COLUMNS)
        items_df = pd.DataFrame(data=items_data, columns=SP_Orders_Retrieval.ITEMS_COLUMNS)
        return orders_df, items_df