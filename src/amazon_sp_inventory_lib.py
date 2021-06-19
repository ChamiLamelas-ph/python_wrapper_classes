from sp_api.api import Inventories
from sp_api.base import Marketplaces
from amazon_sp_constants import NA_MARKETPLACE_COUNTRY_CODESET as nacodes, EUR_MARKETPLACE_COUNTRY_CODESET as eurcodes
from datetime import datetime
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
import pytz


class ASINQuantityRetrieval:

    """
    This class performs a retrieval of the quantities by ASIN for each ASIN in the inventories. This wraps the `Inventories` class in saleweaver's SP-API Python wrapper.

    Parameters
    ----------
    credentials : dict
        The credentials to make the inventory request (default: None). This is an option to set a default `credentials` for all calls to `ASINQuantityRetrieval.retrieve_inventory()`. If you don't specify it here, you will have to specify it in `ASINQuantityRetrieval.retrieve_inventory()`.

    See Also
    --------
    [Inventories in saleweaver's Python wrapper](https://github.com/saleweaver/python-amazon-sp-api/blob/master/sp_api/api/inventories/inventories.py)
    [SP-API for FBA Inventory](https://github.com/amzn/selling-partner-api-docs/blob/main/references/fba-inventory-api/fbaInventory.md)
    """

    def __init__(self, credentials=None):
        self.__credentials = credentials

    def retrieve_inventory(self, marketplace_code, credentials=None):
        """
        Retrieves a DataFrame holding the total quantities for each ASIN returned by GetInventorySummaries from the API.

        Parameters
        ----------
        marketplace_code : str
            The country code of the marketplace to pull inventory information from. 
        credentials : dict
            The credentials to be used when making the pull (default: None). If one is provided, the credentials specified at object construction are ignored. If they are not provided, the credentials specified at object construction are used.

        Returns
        -------
        df : pd.DataFrame
            A DataFrame with the columns `asin`, `marketplace`, `date`, and `quantity`. `quantity` is the inventory quantity for the associated `asin` returned from the API. `date` will be the yyyy-MM-dd date of the method call in the America/Los_Angeles timezone. `marketplace` will be identical to `marketplace_code`.

        Raises
        ------
        RuntimeError
            If `credentials` were not specified at either object construction or in method call.
        """

        if marketplace_code not in nacodes and marketplace_code not in eurcodes:
            raise ValueError('%s is not a valid marketplace code.' %
                             (marketplace_code))

        # construct retrieval object using credentials provided here or instance field
        if not credentials:
            if not self.__credentials:
                raise RuntimeError(
                    'You must specify credentials either at object creation or in credentials in retrieve_inventory().')
            credentials = self.__credentials
        ret = Inventories(credentials=credentials)

        # get marketplace id corresponding to country code
        mplaceid = eval('Marketplaces.%s.marketplace_id' % (marketplace_code))

        # will map asin -> quantities of asin in response
        asin_quantities = defaultdict(lambda: 0)

        # make first request with marketplace id
        resp = ret.get_inventory_summary_marketplace(
            marketplaceIds=mplaceid)

        npage = 0
        last_page = False

        # iterate until we have reached the last page (last page has no next token)
        while not last_page:
            npage += 1

            # summaries in the response is stored in the list 'inventorySummaries' in the response payload
            summaries = resp.payload['inventorySummaries']

            # update quantity for asin in each summary
            for s in tqdm(summaries, desc='Marketplace [%s] Page [%d]' % (marketplace_code, npage)):
                asin_quantities[s['asin']] += s['totalQuantity']

            # pagination is None when there's no next token
            if not resp.pagination:
                last_page = True

            # otherwise, make next page pull using next token in response
            else:
                resp = ret.get_inventory_summary_marketplace(
                    nextToken=resp.pagination['nextToken'])

        # get yyyy-MM-dd date in LA
        date = datetime.now().astimezone(pytz.timezone(
            'America/Los_Angeles')).strftime('%Y-%m-%d')

        # build records that will be put into dataframe, construct frame and return
        records = [[asin, marketplace_code, date, quantity]
                   for asin, quantity in asin_quantities.items()]
        df = pd.DataFrame(records, columns=[
                          'asin', 'marketplace', 'date', 'quantity'])
        return df
