import io
import json
import zipfile
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests

class BalticTransparencyClient:
    """ 
    Form GET requests to Baltic Transparency dashboard. Retrieve relevant data, format it and return as Pandas DataFrame.

    Parameters
    ----------
    start_date : str
        Starting date of data. Format 'yyyy-mm-ddTHH-mm'.
    end_date : str
        Last date of data. Format 'yyyy-mm-ddTHH-mm'.
    """

    def __init__(self, start_date: str, end_date: str):
        # Validate format: 'YYYY-MM-DD'
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Dates must be in format 'YYYY-MM-DD'")
        self.start_date = start_date + 'T00:00'
        self.end_date = end_date + 'T00:00'

    API_BASE_URL = 'https://api-baltic.transparency-dashboard.eu/api/v1'

    def _request(self, endpoint: str, api_id: str) -> requests.Response:
        """Send API request with GET method."""
        params = {
            "id": api_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "output_time_zone": 'EET',
            "output_format": 'json'
        }

        response = requests.get(f"{self.API_BASE_URL}/{endpoint}", params=params)
        if response.status_code == 200:
            return response
        else:
            raise TimeoutError(f"Could not retrieve data. API status code {response.status_code}")

    def _get_data(self, api_id: str) -> str:
        """Retrieve a single report via the /export endpoint."""
        return self._request('export', api_id).text

    def _get_multiple_data(self, api_ids: List[str]) -> Dict[str, dict]:
        """Retrieve several reports via the /export-multiple endpoint (returned as a zip of JSON files)."""
        response = self._request('export-multiple', ','.join(api_ids))
        reports = {}
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            for name in archive.namelist():
                report = json.loads(archive.read(name))
                reports[report['id']] = report
        return reports

    @staticmethod
    def _infer_levels(columns_info: list) -> int:
        """Pick the smallest number of group levels that yields unique column names."""
        if any(col.get('group_level_1') for col in columns_info):
            return 3
        groups = [col['group_level_0'] for col in columns_info]
        if all(col.get('label') is None for col in columns_info) or len(set(groups)) == len(groups):
            return 1
        return 2

    def _unravel_response(self, response: str, levels: int = 1) -> pd.DataFrame:
        """
        Parse and convert the JSON response to a pandas DataFrame.

        Parameters
        ----------
        response : str
            JSON-formatted response string.
        levels : int
            Number of group levels to use in column names (1, 2, or 3).

        Returns
        -------
        pd.DataFrame
            Formatted time-indexed DataFrame.
        """
        response_json = json.loads(response)
        return self._data_to_df(response_json['data'], levels)

    def _data_to_df(self, data: dict, levels: int = 1) -> pd.DataFrame:
        """Convert the 'data' object of a report to a time-indexed DataFrame."""
        # Generate column names based on level depth
        columns_info = data['columns']
        if levels == 3:
            columns_map = {
                col['index']: f"{col['group_level_0']} {col['group_level_1']} {col['label']}" for col in columns_info
            }
        elif levels == 2:
            columns_map = {
                col['index']: f"{col['group_level_0']} {col['label']}" for col in columns_info
            }
        else:  # levels == 1
            columns_map = {
                col['index']: col['group_level_0'] for col in columns_info
            }

        # Build records
        data_records = []
        for entry in data['timeseries']:
            record = {'datetime': entry['from']}
            for idx, value in enumerate(entry['values']):
                if idx in columns_map:
                    record[columns_map[idx]] = value
            data_records.append(record)

        # Create DataFrame
        df = pd.DataFrame(data_records, columns=['datetime', *dict.fromkeys(columns_map.values())])
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert('Europe/Vilnius').dt.tz_localize(None)
        df.set_index('datetime', inplace=True)

        return df

    def multiple_reports_df(self, api_ids: List[str], levels: Optional[Dict[str, int]] = None) -> Dict[str, pd.DataFrame]:
        """
        Get several reports in one request (/export-multiple endpoint).

        Parameters
        ----------
        api_ids : list of str
            Report IDs, e.g. ['imbalance_prices', 'activations_afrr'].
        levels : dict, optional
            Mapping of report ID to number of group levels used in column names (1, 2, or 3).
            Levels are inferred from the report columns for IDs not in the mapping.

        Returns
        -------
        dict of str to pd.DataFrame
            DataFrame per report ID.
        """
        levels = levels or {}
        reports = self._get_multiple_data(api_ids)
        return {
            api_id: self._data_to_df(data, levels.get(api_id, self._infer_levels(data['columns'])))
            for api_id, data in reports.items()
        }

    def procured_balancing_reserve_prices_df(self) -> pd.DataFrame:
        """Get df of TSO procured balancing reserve prices (EUR/MW)."""
        response = self._get_data(api_id='price_procured_reserves')
        return self._unravel_response(response, levels=3)
    
    def procured_balancing_reserve_volumes_df(self) -> pd.DataFrame:
        """Get df of TSO procured balancing reserve volumes (MW)."""
        response = self._get_data(api_id='procured_reserves')
        return self._unravel_response(response, levels=3)

    def cbmp_df(self) -> pd.DataFrame:
        """Get df of cross border marginal prices (EUR/MWh)."""
        response = self._get_data(api_id='cross_border_marginal_price')
        return self._unravel_response(response, levels=3)
    
    def afrr_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max afrr prices."""
        response = self._get_data(api_id='afrr_bid_prices')
        return self._unravel_response(response, levels=3)
    
    def da_mfrr_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max direct mfrr prices."""
        response = self._get_data(api_id='da_mfrr_bid_prices')
        return self._unravel_response(response, levels=3)
    
    def sa_mfrr_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max scheduled mfrr prices."""
        response = self._get_data(api_id='sa_mfrr_bid_prices')
        return self._unravel_response(response, levels=3)
    
    def da_mfrr_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered direct mfrr volumes."""
        response = self._get_data(api_id='da_mfrr_bid_volumes')
        return self._unravel_response(response, levels=2)
    
    def sa_mfrr_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered scheduled mfrr volumes."""
        response = self._get_data(api_id='sa_mfrr_bid_volumes')
        return self._unravel_response(response, levels=2)

    def imbalance_volumes_df(self) -> pd.DataFrame:
        """Get df of imbalance volumes for each country (MWh)."""
        response = self._get_data(api_id='imbalance_volumes_v2')
        return self._unravel_response(response, levels=1)
    
    def activated_afrr_volumes_df(self) -> pd.DataFrame:
        """Get df of activated afrr energy (MWh)"""
        response = self._get_data(api_id='activations_afrr')
        return self._unravel_response(response, levels=2)
    
    def balancing_energy_ref_prices_df(self) -> pd.DataFrame:
        """Get df of balancing energy reference prices (EUR/MWh)."""
        response = self._get_data(api_id='balancing_energy_prices')
        return self._unravel_response(response, levels=2)
    
    def current_balancing_state_df(self) -> pd.DataFrame:
        """Get df of imbalance volumes (MW) for each country with 1 minute resolution."""
        response = self._get_data(api_id='current_balancing_state_v2')
        return self._unravel_response(response, levels=1)
    
    def direction_of_system_balancing_df(self) -> pd.DataFrame:
        """Get df of the directions of system balancing (-1/+1)."""
        response = self._get_data(api_id='direction_of_balancing_v2')
        return self._unravel_response(response, levels=1)
    
    def imbalance_prices_df(self) -> pd.DataFrame:
        """Get df of imabalnce prices calculated by each Baltics TSO."""
        response = self._get_data(api_id='imbalance_prices')
        return self._unravel_response(response, levels=2)
    
    def neutrality_component_df(self) -> pd.DataFrame:
        """Get df the neutrality components."""
        response = self._get_data(api_id='neutrality_component')
        return self._unravel_response(response, levels=1)
    
    def local_marginal_prices_df(self) -> pd.DataFrame:
        """Get df of local marginal mFRR prices (EUR/MWh). Kept for backwards compatibility,
            the 'local_marginal_price' report was replaced by 'local_marginal_price_mfrr'."""
        return self.local_marginal_mfrr_prices_df()

    def local_marginal_mfrr_prices_df(self) -> pd.DataFrame:
        """Get df of local marginal mFRR prices (EUR/MWh)."""
        response = self._get_data(api_id='local_marginal_price_mfrr')
        return self._unravel_response(response, levels=2)
    
    def local_marginal_afrr_prices_df(self) -> pd.DataFrame:
        """Get df of clearing (marginal) aFRR prices (EUR/MWh)."""
        response = self._get_data(api_id='local_marginal_price_afrr')
        return self._unravel_response(response, levels=2)
    
    def normal_activations_da_volumes_df(self) -> pd.DataFrame:
        """Get df of total direct mFRR energy activation volumes."""
        response = self._get_data(api_id='normal_activations_da_mfrr')
        return self._unravel_response(response, levels=2)
    
    def normal_activations_sa_volumes_df(self) -> pd.DataFrame:
        """Get df of total scheduled mFRR energy activation volumes."""
        response = self._get_data(api_id='normal_activations_sa_mfrr')
        return self._unravel_response(response, levels=2)
    
    def normal_activations_total_volumes_df(self) -> pd.DataFrame:
        """Get df of total normal balancing energy activated volumes."""
        response = self._get_data(api_id='normal_activations_total')
        return self._unravel_response(response, levels=2)
    
    def afrr_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered afrr bid volumes (MW)."""
        response = self._get_data(api_id='volumes_afrr')
        return self._unravel_response(response, levels=2)
    
    def unavailable_afrr_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of unavailable afrr bid volumes (MW)."""
        response = self._get_data(api_id='unavailable_afrr_bid_volumes')
        return self._unravel_response(response, levels=2)
    
    def afrr_satisfied_demand_prices_df(self) -> pd.DataFrame:
        """Get df of afrr satisfied demand prices (EUR/MWh)."""
        response = self._get_data(api_id='afrr_satisfied_demand_price')
        return self._unravel_response(response, levels=2)
    
    def satisfied_demand_afrr_df(self) -> pd.DataFrame:
        """Get df of total satisfied demand for balancing purposes from afrr (MWh)."""
        response = self._get_data(api_id='satisfied_demand_afrr')
        return self._unravel_response(response, levels=2)
    
    def netted_exchanged_afrr_volumes_df(self) -> pd.DataFrame:
        """Get df of netted and exchanged afrr volumes (MWh)."""
        response = self._get_data(api_id='netted_exchanged_volumes_afrr')
        return self._unravel_response(response, levels=1)
    
    def mfrr_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max mfrr prices (EUR/MWh)."""
        response = self._get_data(api_id='mfrr_bid_prices')
        return self._unravel_response(response, levels=3)
    
    def mfrr_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered mfrr volumes (MW), 1 hour resolution."""
        response = self._get_data(api_id='mfrr_bid_volumes')
        return self._unravel_response(response, levels=2)
    
    def er_mfrr_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max ER mfrr prices (EUR/MWh)."""
        response = self._get_data(api_id='er_mfrr_bid_prices')
        return self._unravel_response(response, levels=3)
    
    def er_mfrr_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered ER mfrr volumes (MW)."""
        response = self._get_data(api_id='er_mfrr_bid_volumes')
        return self._unravel_response(response, levels=2)
    
    def local_mfrr_er_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered local mfrr (ER) volumes (MW)."""
        response = self._get_data(api_id='local_mfrr_er_bid_volumes')
        return self._unravel_response(response, levels=2)
    
    def unavailable_mfrr_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of unavailable mfrr bid volumes (MW)."""
        response = self._get_data(api_id='unavailable_mfrr_bid_volumes_v2')
        return self._unravel_response(response, levels=2)
    
    def unavailable_mfrr_bid_volumes_legacy_df(self) -> pd.DataFrame:
        """Get df of unavailable mfrr bid volumes (MW), legacy report."""
        response = self._get_data(api_id='unavailable_mfrr_bid_volumes')
        return self._unravel_response(response, levels=2)
    
    def mfrr_satisfied_demand_prices_df(self) -> pd.DataFrame:
        """Get df of mfrr satisfied demand prices (EUR/MWh)."""
        response = self._get_data(api_id='mfrr_satisfied_demand_price')
        return self._unravel_response(response, levels=2)
    
    def netted_exchanged_mfrr_volumes_df(self) -> pd.DataFrame:
        """Get df of netted and exchanged mfrr volumes (MWh)."""
        response = self._get_data(api_id='netted_exchanged_volumes')
        return self._unravel_response(response, levels=1)
    
    def normal_activations_er_volumes_df(self) -> pd.DataFrame:
        """Get df of ER mFRR normal activation volumes (MWh)."""
        response = self._get_data(api_id='normal_activations_er_mfrr')
        return self._unravel_response(response, levels=2)
    
    def normal_activations_local_volumes_df(self) -> pd.DataFrame:
        """Get df of local mFRR normal activation volumes (MWh)."""
        response = self._get_data(api_id='normal_activations_local_mfrr')
        return self._unravel_response(response, levels=2)
    
    def normal_activations_mfrr_volumes_df(self) -> pd.DataFrame:
        """Get df of mFRR normal activation volumes (MWh)."""
        response = self._get_data(api_id='normal_activations_mfrr')
        return self._unravel_response(response, levels=2)
    
    def special_activations_total_volumes_df(self) -> pd.DataFrame:
        """Get df of total special activation volumes (MWh)."""
        response = self._get_data(api_id='special_activations_total')
        return self._unravel_response(response, levels=2)
    
    def special_activations_mfrr_volumes_df(self) -> pd.DataFrame:
        """Get df of total mFRR special activation volumes (MWh)."""
        response = self._get_data(api_id='special_activations_total_mfrr')
        return self._unravel_response(response, levels=2)
    
    def system_services_volumes_df(self) -> pd.DataFrame:
        """Get df of balancing energy volumes activated for system services (MWh)."""
        response = self._get_data(api_id='system_services')
        return self._unravel_response(response, levels=2)
    
    def countertrade_volumes_df(self) -> pd.DataFrame:
        """Get df of balancing energy volumes activated for countertrade (MWh)."""
        response = self._get_data(api_id='countertrade')
        return self._unravel_response(response, levels=2)
    
    def baltic_tso_tso_exchange_df(self) -> pd.DataFrame:
        """Get df of Baltic TSO-TSO exchanged volumes (MWh)."""
        response = self._get_data(api_id='baltic_tso_tso_exchange')
        return self._unravel_response(response, levels=1)
    
    def total_satisfied_demand_from_nonbaltic_tsos_df(self) -> pd.DataFrame:
        """Get df of total satisfied demand from non-Baltic TSOs (MWh)."""
        response = self._get_data(api_id='total_satisfied_demand_from_nonbaltic_tsos')
        return self._unravel_response(response, levels=2)
    
    def procured_balancing_reserve_prices_legacy_df(self) -> pd.DataFrame:
        """Get df of TSO procured balancing reserve prices (EUR/MW/h), 1 hour resolution legacy report."""
        response = self._get_data(api_id='price_procured_reserves_legacy')
        return self._unravel_response(response, levels=3)
    
    def procured_balancing_reserve_volumes_legacy_df(self) -> pd.DataFrame:
        """Get df of TSO procured balancing reserve volumes (MW), 1 hour resolution legacy report."""
        response = self._get_data(api_id='procured_reserves_legacy')
        return self._unravel_response(response, levels=3)
    
    def balancing_reserves_demand_df(self) -> pd.DataFrame:
        """Get df of demand for balancing reserves (MW)."""
        response = self._get_data(api_id='reserves_demand')
        return self._unravel_response(response, levels=3)
    
    def balancing_reserves_demand_legacy_df(self) -> pd.DataFrame:
        """Get df of demand for balancing reserves (MW), 1 hour resolution legacy report."""
        response = self._get_data(api_id='reserves_demand_legacy')
        return self._unravel_response(response, levels=3)
    
    def increased_demands_df(self) -> pd.DataFrame:
        """Get df of increased demands for balancing reserves (MW)."""
        response = self._get_data(api_id='increased_demands')
        return self._unravel_response(response, levels=3)
    
    def fcr_reserve_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max FCR reserve bid prices (EUR/MWh)."""
        response = self._get_data(api_id='fcr_reserve_bid_price')
        return self._unravel_response(response, levels=3)
    
    def afrr_reserve_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max aFRR reserve bid prices (EUR/MWh)."""
        response = self._get_data(api_id='price_afrr_reserve_bids')
        return self._unravel_response(response, levels=3)
    
    def mfrr_reserve_bid_prices_df(self) -> pd.DataFrame:
        """Get df of offered min/max mFRR reserve bid prices (EUR/MWh)."""
        response = self._get_data(api_id='price_mfrr_reserve_bids')
        return self._unravel_response(response, levels=3)
    
    def reserve_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered reserve bid volumes (MW)."""
        response = self._get_data(api_id='volume_of_reserve_bids')
        return self._unravel_response(response, levels=3)
    
    def back_up_reserve_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered back-up reserve bid volumes (MW)."""
        response = self._get_data(api_id='volume_of_back_up_reserve_bids')
        return self._unravel_response(response, levels=3)
    
    def demand_reduction_reserve_bid_volumes_df(self) -> pd.DataFrame:
        """Get df of offered demand reduction reserve bid volumes (MW)."""
        response = self._get_data(api_id='volume_of_demand_reduction_reserve_bids')
        return self._unravel_response(response, levels=3)
    
    def allocated_cb_capacity_df(self) -> pd.DataFrame:
        """Get df of allocated cross-border capacity for balancing reserves (MW)."""
        response = self._get_data(api_id='allocated_cb_capacity')
        return self._unravel_response(response, levels=2)
    
    def allocated_cb_capacity_legacy_df(self) -> pd.DataFrame:
        """Get df of allocated cross-border capacity for balancing reserves (MW), 1 hour resolution legacy report."""
        response = self._get_data(api_id='allocated_cb_capacity_legacy')
        return self._unravel_response(response, levels=2)
    
    def allocated_capacity_limits_df(self) -> pd.DataFrame:
        """Get df of allocated capacity limits (%)."""
        response = self._get_data(api_id='allocated_capacity_limit')
        return self._unravel_response(response, levels=2)
    
    def cross_border_capacity_limits_df(self) -> pd.DataFrame:
        """Get df of cross-border capacity limits (MW)."""
        response = self._get_data(api_id='cross_border_capacity_limits_v2')
        return self._unravel_response(response, levels=2)
    
    def cross_zonal_capacities_df(self) -> pd.DataFrame:
        """Get df of cross-zonal capacities within the balancing timeframe (MW)."""
        response = self._get_data(api_id='cross_zonal_capacities')
        return self._unravel_response(response, levels=2)
    
    def applied_mark_up_df(self) -> pd.DataFrame:
        """Get df of applied mark-ups (EUR/MWh)."""
        response = self._get_data(api_id='applied_mark_up')
        return self._unravel_response(response, levels=1)
    
    def imbalance_volumes_legacy_df(self) -> pd.DataFrame:
        """Get df of imbalance volumes (MWh), legacy report incl. Baltics total."""
        response = self._get_data(api_id='imbalance_volumes')
        return self._unravel_response(response, levels=1)
    
    def current_balancing_state_legacy_df(self) -> pd.DataFrame:
        """Get df of imbalance volumes (MW) with 1 minute resolution, legacy report incl. Baltics total."""
        response = self._get_data(api_id='current_balancing_state')
        return self._unravel_response(response, levels=1)
    
    def direction_of_system_balancing_legacy_df(self) -> pd.DataFrame:
        """Get df of the Baltics direction of system balancing (-1/+1), legacy report."""
        response = self._get_data(api_id='direction_of_balancing')
        return self._unravel_response(response, levels=1)
    
    def neutrality_component_legacy_df(self) -> pd.DataFrame:
        """Get df of the Baltics neutrality component (EUR/MWh), legacy report."""
        response = self._get_data(api_id='neutrality')
        return self._unravel_response(response, levels=1)
    
    def total_satisfied_demand_for_balancing_purposes_df(self) -> pd.DataFrame:
        """Get df total satisfied demands for balancing purposes (MWh)."""
        response = self._get_data(api_id='total_satisfied_demand_for_balancing_purposes')
        df = self._unravel_response(response, levels=2)
        return df
    
    def total_satisfied_demand_for_balancing_purposes_mod_df(self) -> pd.DataFrame:
        """Get df total satisfied demands for balancing purposes. Convertet to MW and
            total Baltics demand computed and returned."""
        response = self._get_data(api_id='total_satisfied_demand_for_balancing_purposes')
        df = self._unravel_response(response, levels=2)

        # Apply processing logic
        downward_columns = [col for col in df.columns if "Downward" in col]
        df[downward_columns] = df[downward_columns].apply(pd.to_numeric, errors='coerce') * -1
        df = df.dropna()
        df['Baltics_net'] = df.sum(axis=1)
        df *= 4
        return df
