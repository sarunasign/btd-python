import requests
import json
import pandas as pd
from datetime import datetime

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

    def _get_data(self, api_id: str) -> str:
        """Send API request with GET method."""
        api_url = 'https://api-baltic.transparency-dashboard.eu/api/v1/export'

        params = {
            "id": api_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "output_time_zone": 'EET',
            "output_format": 'json'
        }

        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            return response.text
        else:
            raise TimeoutError(f"Could not retrieve data. API status code {response.status_code}")

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

        # Generate column names based on level depth
        columns_info = response_json['data']['columns']
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
        for entry in response_json['data']['timeseries']:
            record = {'datetime': entry['from']}
            for idx, value in enumerate(entry['values']):
                if idx in columns_map:
                    record[columns_map[idx]] = value
            data_records.append(record)

        # Create DataFrame
        df = pd.DataFrame(data_records)
        df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_convert('Europe/Vilnius').dt.tz_localize(None)
        df.set_index('datetime', inplace=True)

        return df

    def procured_balancing_reserve_prices_df(self) -> pd.DataFrame:
        """Get df of TSO procured balancing reserve prices (EUR/MW)."""
        response = self._get_data(api_id='price_procured_reserves')
        return self._unravel_response(response, levels=3)

    def cbmp_df(self) -> pd.DataFrame:
        """Get df of cross border marginal prices (EUR/MWh)."""
        response = self._get_data(api_id='cross_border_marginal_price')
        return self._unravel_response(response, levels=3)

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
        return self._unravel_response(response, levels=1)
    
    def local_marginal_prices_df(self) -> pd.DataFrame:
        """Get df of local marginal prices."""
        response = self._get_data(api_id='local_marginal_price')
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