CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.InterpolatePVTCompletionTest(completion VARCHAR(32), pressure FLOAT, vrr_date DATE)
RETURNS TABLE (
    PRESSURE FLOAT,
    OIL_FORMATION_VOLUME_FACTOR FLOAT,
    GAS_FORMATION_VOLUME_FACTOR FLOAT,
    WATER_FORMATION_VOLUME_FACTOR FLOAT,
    SOLUTION_GAS_OIL_RATIO FLOAT,
    VISCOSITY_OIL FLOAT,
    VISCOSITY_WATER FLOAT,
    VISCOSITY_GAS FLOAT,
    INJECTED_GAS_FORMATION_VOLUME_FACTOR FLOAT,
    INJECTED_WATER_FORMATION_VOLUME_FACTOR FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'InterpolatePVTCompletionTest'
AS
$$
from snowflake.snowpark.functions import col, last_day, to_date
from snowflake.snowpark import Session
from datetime import datetime
from typing import Iterable, Tuple
import uuid

class InterpolatePVTCompletionTest:
    def process(
        self, 
        completion: str, 
        pressure: float, 
        vrr_date: datetime
    ) -> Iterable[Tuple[float, float, float, float, float, float, float, float, float, float]]:
        # Get the Snowpark session
        session = Session.builder.getOrCreate()

        # Step 1: Compute LAST_DAY(vrr_date, 'MONTH')
        last_day_df = session.sql(f"SELECT LAST_DAY(TO_DATE('{vrr_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD'), 'MONTH') AS last_day")
        last_day = last_day_df.collect()[0]['LAST_DAY']

        # Step 2: Fetch BasePVTData
        base_pvt_query = """
            SELECT 
                ID_COMPLETION,
                TEST_DATE,
                CAST(PRESSURE AS FLOAT) AS PRESSURE,
                CAST(OIL_FORMATION_VOLUME_FACTOR AS FLOAT) AS OIL_FORMATION_VOLUME_FACTOR,
                CAST(GAS_FORMATION_VOLUME_FACTOR AS FLOAT) AS GAS_FORMATION_VOLUME_FACTOR,
                CAST(WATER_FORMATION_VOLUME_FACTOR AS FLOAT) AS WATER_FORMATION_VOLUME_FACTOR,
                CAST(SOLUTION_GAS_OIL_RATIO AS FLOAT) AS SOLUTION_GAS_OIL_RATIO,
                CAST(VISCOSITY_OIL AS FLOAT) AS VISCOSITY_OIL,
                CAST(VISCOSITY_WATER AS FLOAT) AS VISCOSITY_WATER,
                CAST(VISCOSITY_GAS AS FLOAT) AS VISCOSITY_GAS,
                CAST(INJECTED_GAS_FORMATION_VOLUME_FACTOR AS FLOAT) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                CAST(INJECTED_WATER_FORMATION_VOLUME_FACTOR AS FLOAT) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS
            WHERE ID_COMPLETION = %s
              AND TEST_DATE <= %s
            ORDER BY TEST_DATE DESC
        """
        base_pvt_df = session.sql(base_pvt_query, params=[completion, last_day])
        base_pvt_data = [row.asDict() for row in base_pvt_df.collect()]

        # Step 3: Compute END_DATE for each record
        base_pvt_data_asc = sorted(base_pvt_data, key=lambda x: x['TEST_DATE'])
        pvt_with_end_date = []
        for i, record in enumerate(base_pvt_data_asc):
            next_test_date = base_pvt_data_asc[i + 1]['TEST_DATE'] if i < len(base_pvt_data_asc) - 1 and base_pvt_data_asc[i + 1]['ID_COMPLETION'] == record['ID_COMPLETION'] else datetime(9999, 12, 31)
            if last_day < next_test_date:
                record_copy = record.copy()
                record_copy['END_DATE'] = next_test_date
                pvt_with_end_date.append(record_copy)

        # Step 4: Sort by TEST_DATE DESC for exact match
        pvt_with_end_date.sort(key=lambda x: x['TEST_DATE'], reverse=True)

        # Step 5: Find ExactMatch
        exact_match = None
        for record in pvt_with_end_date:
            if (abs(record['PRESSURE'] - pressure) < 1e-5 and 
                record['ID_COMPLETION'] == completion and 
                record['TEST_DATE'] <= last_day):
                exact_match = record
                break

        # Step 6: Sort by PRESSURE to find bounds
        pvt_sorted_by_pressure = sorted(pvt_with_end_date, key=lambda x: x['PRESSURE'])

        # Step 7: Find Lowerbound
        lowerbound = None
        lowerbound_diff = float('inf')
        for record in pvt_sorted_by_pressure:
            if (record['PRESSURE'] < pressure and 
                record['ID_COMPLETION'] == completion and 
                record['TEST_DATE'] <= last_day):
                diff = pressure - record['PRESSURE']
                if diff < lowerbound_diff:
                    lowerbound = record
                    lowerbound_diff = diff

        # Step 8: Find Upperbound
        upperbound = None
        upperbound_diff = float('inf')
        for record in pvt_sorted_by_pressure:
            if (record['PRESSURE'] > pressure and 
                record['ID_COMPLETION'] == completion and 
                record['TEST_DATE'] <= last_day):
                diff = record['PRESSURE'] - pressure
                if diff < upperbound_diff:
                    upperbound = record
                    upperbound_diff = diff

        # Step 9: Find the two lowest pressures for extrapolation below
        lowest_bound = None
        second_lowest_bound = None
        lowest_pressure = float('inf')
        second_lowest_pressure = float('inf')
        for record in pvt_sorted_by_pressure:
            if (record['ID_COMPLETION'] == completion and 
                record['TEST_DATE'] <= last_day):
                if record['PRESSURE'] < lowest_pressure:
                    second_lowest_bound = lowest_bound
                    second_lowest_pressure = lowest_pressure
                    lowest_bound = record
                    lowest_pressure = record['PRESSURE']
                elif record['PRESSURE'] < second_lowest_pressure and record['PRESSURE'] > lowest_pressure:
                    second_lowest_bound = record
                    second_lowest_pressure = record['PRESSURE']

        # Step 10: Find the two highest pressures for extrapolation above
        highest_bound = None
        second_highest_bound = None
        highest_pressure = float('-inf')
        second_highest_pressure = float('-inf')
        for record in pvt_sorted_by_pressure:
            if (record['ID_COMPLETION'] == completion and 
                record['TEST_DATE'] <= last_day):
                if record['PRESSURE'] > highest_pressure:
                    second_highest_bound = highest_bound
                    second_highest_pressure = highest_pressure
                    highest_bound = record
                    highest_pressure = record['PRESSURE']
                elif record['PRESSURE'] > second_highest_pressure and record['PRESSURE'] < highest_pressure:
                    second_highest_bound = record
                    second_highest_pressure = record['PRESSURE']

        # Step 11: Compute interpolated or extrapolated values
        result = {}
        if exact_match:
            result = {
                'PRESSURE': exact_match['PRESSURE'],
                'OIL_FORMATION_VOLUME_FACTOR': exact_match['OIL_FORMATION_VOLUME_FACTOR'],
                'GAS_FORMATION_VOLUME_FACTOR': exact_match['GAS_FORMATION_VOLUME_FACTOR'],
                'WATER_FORMATION_VOLUME_FACTOR': exact_match['WATER_FORMATION_VOLUME_FACTOR'],
                'SOLUTION_GAS_OIL_RATIO': exact_match['SOLUTION_GAS_OIL_RATIO'],
                'VISCOSITY_OIL': exact_match['VISCOSITY_OIL'],
                'VISCOSITY_WATER': exact_match['VISCOSITY_WATER'],
                'VISCOSITY_GAS': exact_match['VISCOSITY_GAS'],
                'INJECTED_GAS_FORMATION_VOLUME_FACTOR': exact_match['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                'INJECTED_WATER_FORMATION_VOLUME_FACTOR': exact_match['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            }
        elif lowerbound and upperbound:
            interpolate_query = """
                SELECT 
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(%s, %s, %s, %s, %s) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            """
            interpolate_params = [
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['OIL_FORMATION_VOLUME_FACTOR'], upperbound['OIL_FORMATION_VOLUME_FACTOR'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['GAS_FORMATION_VOLUME_FACTOR'], upperbound['GAS_FORMATION_VOLUME_FACTOR'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['WATER_FORMATION_VOLUME_FACTOR'], upperbound['WATER_FORMATION_VOLUME_FACTOR'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['SOLUTION_GAS_OIL_RATIO'], upperbound['SOLUTION_GAS_OIL_RATIO'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['VISCOSITY_OIL'], upperbound['VISCOSITY_OIL'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['VISCOSITY_WATER'], upperbound['VISCOSITY_WATER'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['VISCOSITY_GAS'], upperbound['VISCOSITY_GAS'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['INJECTED_GAS_FORMATION_VOLUME_FACTOR'], upperbound['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                pressure, lowerbound['PRESSURE'], upperbound['PRESSURE'], lowerbound['INJECTED_WATER_FORMATION_VOLUME_FACTOR'], upperbound['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            ]
            interpolate_df = session.sql(interpolate_query, params=interpolate_params)
            interpolate_result = interpolate_df.collect()[0]
            result = {
                'PRESSURE': pressure,
                'OIL_FORMATION_VOLUME_FACTOR': interpolate_result['OIL_FORMATION_VOLUME_FACTOR'],
                'GAS_FORMATION_VOLUME_FACTOR': interpolate_result['GAS_FORMATION_VOLUME_FACTOR'],
                'WATER_FORMATION_VOLUME_FACTOR': interpolate_result['WATER_FORMATION_VOLUME_FACTOR'],
                'SOLUTION_GAS_OIL_RATIO': interpolate_result['SOLUTION_GAS_OIL_RATIO'],
                'VISCOSITY_OIL': interpolate_result['VISCOSITY_OIL'],
                'VISCOSITY_WATER': interpolate_result['VISCOSITY_WATER'],
                'VISCOSITY_GAS': interpolate_result['VISCOSITY_GAS'],
                'INJECTED_GAS_FORMATION_VOLUME_FACTOR': interpolate_result['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                'INJECTED_WATER_FORMATION_VOLUME_FACTOR': interpolate_result['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            }
        elif pressure < lowest_pressure and lowest_bound and second_lowest_bound:
            extrapolate_query = """
                SELECT 
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            """
            extrapolate_params = [
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['OIL_FORMATION_VOLUME_FACTOR'], second_lowest_bound['OIL_FORMATION_VOLUME_FACTOR'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['GAS_FORMATION_VOLUME_FACTOR'], second_lowest_bound['GAS_FORMATION_VOLUME_FACTOR'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['WATER_FORMATION_VOLUME_FACTOR'], second_lowest_bound['WATER_FORMATION_VOLUME_FACTOR'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['SOLUTION_GAS_OIL_RATIO'], second_lowest_bound['SOLUTION_GAS_OIL_RATIO'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['VISCOSITY_OIL'], second_lowest_bound['VISCOSITY_OIL'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['VISCOSITY_WATER'], second_lowest_bound['VISCOSITY_WATER'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['VISCOSITY_GAS'], second_lowest_bound['VISCOSITY_GAS'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['INJECTED_GAS_FORMATION_VOLUME_FACTOR'], second_lowest_bound['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                pressure, lowest_bound['PRESSURE'], second_lowest_bound['PRESSURE'], lowest_bound['INJECTED_WATER_FORMATION_VOLUME_FACTOR'], second_lowest_bound['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            ]
            extrapolate_df = session.sql(extrapolate_query, params=extrapolate_params)
            extrapolate_result = extrapolate_df.collect()[0]
            result = {
                'PRESSURE': pressure,
                'OIL_FORMATION_VOLUME_FACTOR': extrapolate_result['OIL_FORMATION_VOLUME_FACTOR'],
                'GAS_FORMATION_VOLUME_FACTOR': extrapolate_result['GAS_FORMATION_VOLUME_FACTOR'],
                'WATER_FORMATION_VOLUME_FACTOR': extrapolate_result['WATER_FORMATION_VOLUME_FACTOR'],
                'SOLUTION_GAS_OIL_RATIO': extrapolate_result['SOLUTION_GAS_OIL_RATIO'],
                'VISCOSITY_OIL': extrapolate_result['VISCOSITY_OIL'],
                'VISCOSITY_WATER': extrapolate_result['VISCOSITY_WATER'],
                'VISCOSITY_GAS': extrapolate_result['VISCOSITY_GAS'],
                'INJECTED_GAS_FORMATION_VOLUME_FACTOR': extrapolate_result['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                'INJECTED_WATER_FORMATION_VOLUME_FACTOR': extrapolate_result['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            }
        elif pressure > highest_pressure and highest_bound and second_highest_bound:
            extrapolate_query = """
                SELECT 
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(%s, %s, %s, %s, %s) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            """
            extrapolate_params = [
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['OIL_FORMATION_VOLUME_FACTOR'], second_highest_bound['OIL_FORMATION_VOLUME_FACTOR'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['GAS_FORMATION_VOLUME_FACTOR'], second_highest_bound['GAS_FORMATION_VOLUME_FACTOR'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['WATER_FORMATION_VOLUME_FACTOR'], second_highest_bound['WATER_FORMATION_VOLUME_FACTOR'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['SOLUTION_GAS_OIL_RATIO'], second_highest_bound['SOLUTION_GAS_OIL_RATIO'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['VISCOSITY_OIL'], second_highest_bound['VISCOSITY_OIL'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['VISCOSITY_WATER'], second_highest_bound['VISCOSITY_WATER'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['VISCOSITY_GAS'], second_highest_bound['VISCOSITY_GAS'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['INJECTED_GAS_FORMATION_VOLUME_FACTOR'], second_highest_bound['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                pressure, highest_bound['PRESSURE'], second_highest_bound['PRESSURE'], highest_bound['INJECTED_WATER_FORMATION_VOLUME_FACTOR'], second_highest_bound['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            ]
            extrapolate_df = session.sql(extrapolate_query, params=extrapolate_params)
            extrapolate_result = extrapolate_df.collect()[0]
            result = {
                'PRESSURE': pressure,
                'OIL_FORMATION_VOLUME_FACTOR': extrapolate_result['OIL_FORMATION_VOLUME_FACTOR'],
                'GAS_FORMATION_VOLUME_FACTOR': extrapolate_result['GAS_FORMATION_VOLUME_FACTOR'],
                'WATER_FORMATION_VOLUME_FACTOR': extrapolate_result['WATER_FORMATION_VOLUME_FACTOR'],
                'SOLUTION_GAS_OIL_RATIO': extrapolate_result['SOLUTION_GAS_OIL_RATIO'],
                'VISCOSITY_OIL': extrapolate_result['VISCOSITY_OIL'],
                'VISCOSITY_WATER': extrapolate_result['VISCOSITY_WATER'],
                'VISCOSITY_GAS': extrapolate_result['VISCOSITY_GAS'],
                'INJECTED_GAS_FORMATION_VOLUME_FACTOR': extrapolate_result['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                'INJECTED_WATER_FORMATION_VOLUME_FACTOR': extrapolate_result['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            }
        elif lowest_bound:
            result = {
                'PRESSURE': pressure,
                'OIL_FORMATION_VOLUME_FACTOR': lowest_bound['OIL_FORMATION_VOLUME_FACTOR'],
                'GAS_FORMATION_VOLUME_FACTOR': lowest_bound['GAS_FORMATION_VOLUME_FACTOR'],
                'WATER_FORMATION_VOLUME_FACTOR': lowest_bound['WATER_FORMATION_VOLUME_FACTOR'],
                'SOLUTION_GAS_OIL_RATIO': lowest_bound['SOLUTION_GAS_OIL_RATIO'],
                'VISCOSITY_OIL': lowest_bound['VISCOSITY_OIL'],
                'VISCOSITY_WATER': lowest_bound['VISCOSITY_WATER'],
                'VISCOSITY_GAS': lowest_bound['VISCOSITY_GAS'],
                'INJECTED_GAS_FORMATION_VOLUME_FACTOR': lowest_bound['INJECTED_GAS_FORMATION_VOLUME_FACTOR'],
                'INJECTED_WATER_FORMATION_VOLUME_FACTOR': lowest_bound['INJECTED_WATER_FORMATION_VOLUME_FACTOR']
            }
        else:
            result = {
                'PRESSURE': pressure,
                'OIL_FORMATION_VOLUME_FACTOR': None,
                'GAS_FORMATION_VOLUME_FACTOR': None,
                'WATER_FORMATION_VOLUME_FACTOR': None,
                'SOLUTION_GAS_OIL_RATIO': None,
                'VISCOSITY_OIL': None,
                'VISCOSITY_WATER': None,
                'VISCOSITY_GAS': None,
                'INJECTED_GAS_FORMATION_VOLUME_FACTOR': None,
                'INJECTED_WATER_FORMATION_VOLUME_FACTOR': None
            }

        # Step 12: Round the results to 5 decimal places
        rounded_result = (
            round(result['PRESSURE'], 5),
            round(result['OIL_FORMATION_VOLUME_FACTOR'], 5) if result['OIL_FORMATION_VOLUME_FACTOR'] is not None else None,
            round(result['GAS_FORMATION_VOLUME_FACTOR'], 5) if result['GAS_FORMATION_VOLUME_FACTOR'] is not None else None,
            round(result['WATER_FORMATION_VOLUME_FACTOR'], 5) if result['WATER_FORMATION_VOLUME_FACTOR'] is not None else None,
            round(result['SOLUTION_GAS_OIL_RATIO'], 5) if result['SOLUTION_GAS_OIL_RATIO'] is not None else None,
            round(result['VISCOSITY_OIL'], 5) if result['VISCOSITY_OIL'] is not None else None,
            round(result['VISCOSITY_WATER'], 5) if result['VISCOSITY_WATER'] is not None else None,
            round(result['VISCOSITY_GAS'], 5) if result['VISCOSITY_GAS'] is not None else None,
            round(result['INJECTED_GAS_FORMATION_VOLUME_FACTOR'], 5) if result['INJECTED_GAS_FORMATION_VOLUME_FACTOR'] is not None else None,
            round(result['INJECTED_WATER_FORMATION_VOLUME_FACTOR'], 5) if result['INJECTED_WATER_FORMATION_VOLUME_FACTOR'] is not None else None
        )

        # Step 13: Yield the result
        yield rounded_result
$$;
