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
LANGUAGE JAVASCRIPT
AS
$$
{
    processRow: function (row, rowWriter, context) {
        // Extract input parameters
        const completion = row.COMPLETION;
        const pressure = row.PRESSURE;
        const vrr_date = row.VRR_DATE;

        // Step 1: Compute LAST_DAY(vrr_date, 'MONTH')
        const lastDayQuery = `
            SELECT LAST_DAY(TO_DATE(:1, 'YYYY-MM-DD'), 'MONTH') AS last_day
        `;
        const lastDayResult = snowflake.execute({ sqlText: lastDayQuery, binds: [vrr_date] });
        lastDayResult.next();
        const lastDay = lastDayResult.getColumnValue(1);

        // Step 2: Fetch BasePVTData
        const basePVTQuery = `
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
            WHERE ID_COMPLETION = :1
              AND TEST_DATE <= :2
            ORDER BY TEST_DATE DESC
        `;
        const basePVTResult = snowflake.execute({ sqlText: basePVTQuery, binds: [completion, lastDay] });
        const basePVTData = [];
        while (basePVTResult.next()) {
            basePVTData.push({
                ID_COMPLETION: basePVTResult.getColumnValue(1),
                TEST_DATE: basePVTResult.getColumnValue(2),
                PRESSURE: basePVTResult.getColumnValue(3),
                OIL_FORMATION_VOLUME_FACTOR: basePVTResult.getColumnValue(4),
                GAS_FORMATION_VOLUME_FACTOR: basePVTResult.getColumnValue(5),
                WATER_FORMATION_VOLUME_FACTOR: basePVTResult.getColumnValue(6),
                SOLUTION_GAS_OIL_RATIO: basePVTResult.getColumnValue(7),
                VISCOSITY_OIL: basePVTResult.getColumnValue(8),
                VISCOSITY_WATER: basePVTResult.getColumnValue(9),
                VISCOSITY_GAS: basePVTResult.getColumnValue(10),
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: basePVTResult.getColumnValue(11),
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: basePVTResult.getColumnValue(12)
            });
        }

        // Step 3: Compute END_DATE for each record
        const basePVTDataAsc = [...basePVTData].sort((a, b) => new Date(a.TEST_DATE) - new Date(b.TEST_DATE));
        const pvtWithEndDate = [];
        for (let i = 0; i < basePVTDataAsc.length; i++) {
            const record = basePVTDataAsc[i];
            let nextTestDate = null;
            if (i < basePVTDataAsc.length - 1 && basePVTDataAsc[i + 1].ID_COMPLETION === record.ID_COMPLETION) {
                nextTestDate = basePVTDataAsc[i + 1].TEST_DATE;
            }
            const endDate = nextTestDate || '9999-12-31';

            if (new Date(lastDay) < new Date(endDate)) {
                pvtWithEndDate.push({
                    ...record,
                    END_DATE: endDate
                });
            }
        }

        // Step 4: Sort pvtWithEndDate by TEST_DATE DESC for exact match
        pvtWithEndDate.sort((a, b) => new Date(b.TEST_DATE) - new Date(a.TEST_DATE));

        // Step 5: Find ExactMatch
        let exactMatch = null;
        for (const record of pvtWithEndDate) {
            if (record.PRESSURE === pressure && record.ID_COMPLETION === completion && new Date(record.TEST_DATE) <= new Date(lastDay)) {
                exactMatch = record;
                break;
            }
        }

        // Step 6: Sort by PRESSURE to find bounds
        const pvtSortedByPressure = [...pvtWithEndDate].sort((a, b) => a.PRESSURE - b.PRESSURE);

        // Step 7: Find Lowerbound
        let lowerbound = null;
        let lowerboundDiff = Infinity;
        for (const record of pvtSortedByPressure) {
            if (record.PRESSURE < pressure && record.ID_COMPLETION === completion && new Date(record.TEST_DATE) <= new Date(lastDay)) {
                const diff = pressure - record.PRESSURE;
                if (diff < lowerboundDiff) {
                    lowerbound = record;
                    lowerboundDiff = diff;
                }
            }
        }

        // Step 8: Find Upperbound
        let upperbound = null;
        let upperboundDiff = Infinity;
        for (const record of pvtSortedByPressure) {
            if (record.PRESSURE > pressure && record.ID_COMPLETION === completion && new Date(record.TEST_DATE) <= new Date(lastDay)) {
                const diff = record.PRESSURE - pressure;
                if (diff < upperboundDiff) {
                    upperbound = record;
                    upperboundDiff = diff;
                }
            }
        }

        // Step 9: Find the two lowest pressures for extrapolation below the range
        let lowestBound = null;
        let secondLowestBound = null;
        let lowestPressure = Infinity;
        let secondLowestPressure = Infinity;
        for (const record of pvtSortedByPressure) {
            if (record.ID_COMPLETION === completion && new Date(record.TEST_DATE) <= new Date(lastDay)) {
                if (record.PRESSURE < lowestPressure) {
                    secondLowestBound = lowestBound;
                    secondLowestPressure = lowestPressure;
                    lowestBound = record;
                    lowestPressure = record.PRESSURE;
                } else if (record.PRESSURE < secondLowestPressure && record.PRESSURE > lowestPressure) {
                    secondLowestBound = record;
                    secondLowestPressure = record.PRESSURE;
                }
            }
        }

        // Step 10: Find the two highest pressures for extrapolation above the range
        let highestBound = null;
        let secondHighestBound = null;
        let highestPressure = -Infinity;
        let secondHighestPressure = -Infinity;
        for (const record of pvtSortedByPressure) {
            if (record.ID_COMPLETION === completion && new Date(record.TEST_DATE) <= new Date(lastDay)) {
                if (record.PRESSURE > highestPressure) {
                    secondHighestBound = highestBound;
                    secondHighestPressure = highestPressure;
                    highestBound = record;
                    highestPressure = record.PRESSURE;
                } else if (record.PRESSURE > secondHighestPressure && record.PRESSURE < highestPressure) {
                    secondHighestBound = record;
                    secondHighestPressure = record.PRESSURE;
                }
            }
        }

        // Step 11: Compute the interpolated or extrapolated values
        let result = {};
        if (exactMatch) {
            result = {
                PRESSURE: exactMatch.PRESSURE,
                OIL_FORMATION_VOLUME_FACTOR: exactMatch.OIL_FORMATION_VOLUME_FACTOR,
                GAS_FORMATION_VOLUME_FACTOR: exactMatch.GAS_FORMATION_VOLUME_FACTOR,
                WATER_FORMATION_VOLUME_FACTOR: exactMatch.WATER_FORMATION_VOLUME_FACTOR,
                SOLUTION_GAS_OIL_RATIO: exactMatch.SOLUTION_GAS_OIL_RATIO,
                VISCOSITY_OIL: exactMatch.VISCOSITY_OIL,
                VISCOSITY_WATER: exactMatch.VISCOSITY_WATER,
                VISCOSITY_GAS: exactMatch.VISCOSITY_GAS,
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: exactMatch.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: exactMatch.INJECTED_WATER_FORMATION_VOLUME_FACTOR
            };
        } else if (lowerbound && upperbound) {
            const interpolateQuery = `
                SELECT 
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :4, :5) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :6, :7) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :8, :9) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :10, :11) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :12, :13) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :14, :15) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :16, :17) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :18, :19) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(:1, :2, :3, :20, :21) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            `;
            const interpolateResult = snowflake.execute({ sqlText: interpolateQuery, binds: [
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE,
                lowerbound.OIL_FORMATION_VOLUME_FACTOR, upperbound.OIL_FORMATION_VOLUME_FACTOR,
                lowerbound.GAS_FORMATION_VOLUME_FACTOR, upperbound.GAS_FORMATION_VOLUME_FACTOR,
                lowerbound.WATER_FORMATION_VOLUME_FACTOR, upperbound.WATER_FORMATION_VOLUME_FACTOR,
                lowerbound.SOLUTION_GAS_OIL_RATIO, upperbound.SOLUTION_GAS_OIL_RATIO,
                lowerbound.VISCOSITY_OIL, upperbound.VISCOSITY_OIL,
                lowerbound.VISCOSITY_WATER, upperbound.VISCOSITY_WATER,
                lowerbound.VISCOSITY_GAS, upperbound.VISCOSITY_GAS,
                lowerbound.INJECTED_GAS_FORMATION_VOLUME_FACTOR, upperbound.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                lowerbound.INJECTED_WATER_FORMATION_VOLUME_FACTOR, upperbound.INJECTED_WATER_FORMATION_VOLUME_FACTOR
            ]});
            interpolateResult.next();
            result = {
                PRESSURE: pressure,
                OIL_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue(1),
                GAS_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue(2),
                WATER_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue(3),
                SOLUTION_GAS_OIL_RATIO: interpolateResult.getColumnValue(4),
                VISCOSITY_OIL: interpolateResult.getColumnValue(5),
                VISCOSITY_WATER: interpolateResult.getColumnValue(6),
                VISCOSITY_GAS: interpolateResult.getColumnValue(7),
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue(8),
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue(9)
            };
        } else if (pressure < lowestPressure && lowestBound && secondLowestBound) {
            const extrapolateQuery = `
                SELECT 
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :4, :5) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :6, :7) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :8, :9) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :10, :11) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :12, :13) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :14, :15) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :16, :17) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :18, :19) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :20, :21) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            `;
            const extrapolateResult = snowflake.execute({ sqlText: extrapolateQuery, binds: [
                pressure, lowestBound.PRESSURE, secondLowestBound.PRESSURE,
                lowestBound.OIL_FORMATION_VOLUME_FACTOR, secondLowestBound.OIL_FORMATION_VOLUME_FACTOR,
                lowestBound.GAS_FORMATION_VOLUME_FACTOR, secondLowestBound.GAS_FORMATION_VOLUME_FACTOR,
                lowestBound.WATER_FORMATION_VOLUME_FACTOR, secondLowestBound.WATER_FORMATION_VOLUME_FACTOR,
                lowestBound.SOLUTION_GAS_OIL_RATIO, secondLowestBound.SOLUTION_GAS_OIL_RATIO,
                lowestBound.VISCOSITY_OIL, secondLowestBound.VISCOSITY_OIL,
                lowestBound.VISCOSITY_WATER, secondLowestBound.VISCOSITY_WATER,
                lowestBound.VISCOSITY_GAS, secondLowestBound.VISCOSITY_GAS,
                lowestBound.INJECTED_GAS_FORMATION_VOLUME_FACTOR, secondLowestBound.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                lowestBound.INJECTED_WATER_FORMATION_VOLUME_FACTOR, secondLowestBound.INJECTED_WATER_FORMATION_VOLUME_FACTOR
            ]});
            extrapolateResult.next();
            result = {
                PRESSURE: pressure,
                OIL_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(1),
                GAS_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(2),
                WATER_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(3),
                SOLUTION_GAS_OIL_RATIO: extrapolateResult.getColumnValue(4),
                VISCOSITY_OIL: extrapolateResult.getColumnValue(5),
                VISCOSITY_WATER: extrapolateResult.getColumnValue(6),
                VISCOSITY_GAS: extrapolateResult.getColumnValue(7),
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(8),
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(9)
            };
        } else if (pressure > highestPressure && highestBound && secondHighestBound) {
            const extrapolateQuery = `
                SELECT 
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :4, :5) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :6, :7) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :8, :9) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :10, :11) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :12, :13) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :14, :15) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :16, :17) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :18, :19) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :20, :21) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            `;
            const extrapolateResult = snowflake.execute({ sqlText: extrapolateQuery, binds: [
                pressure, highestBound.PRESSURE, secondHighestBound.PRESSURE,
                highestBound.OIL_FORMATION_VOLUME_FACTOR, secondHighestBound.OIL_FORMATION_VOLUME_FACTOR,
                highestBound.GAS_FORMATION_VOLUME_FACTOR, secondHighestBound.GAS_FORMATION_VOLUME_FACTOR,
                highestBound.WATER_FORMATION_VOLUME_FACTOR, secondHighestBound.WATER_FORMATION_VOLUME_FACTOR,
                highestBound.SOLUTION_GAS_OIL_RATIO, secondHighestBound.SOLUTION_GAS_OIL_RATIO,
                highestBound.VISCOSITY_OIL, secondHighestBound.VISCOSITY_OIL,
                highestBound.VISCOSITY_WATER, secondHighestBound.VISCOSITY_WATER,
                highestBound.VISCOSITY_GAS, secondHighestBound.VISCOSITY_GAS,
                highestBound.INJECTED_GAS_FORMATION_VOLUME_FACTOR, secondHighestBound.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                highestBound.INJECTED_WATER_FORMATION_VOLUME_FACTOR, secondHighestBound.INJECTED_WATER_FORMATION_VOLUME_FACTOR
            ]});
            extrapolateResult.next();
            result = {
                PRESSURE: pressure,
                OIL_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(1),
                GAS_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(2),
                WATER_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(3),
                SOLUTION_GAS_OIL_RATIO: extrapolateResult.getColumnValue(4),
                VISCOSITY_OIL: extrapolateResult.getColumnValue(5),
                VISCOSITY_WATER: extrapolateResult.getColumnValue(6),
                VISCOSITY_GAS: extrapolateResult.getColumnValue(7),
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(8),
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue(9)
            };
        } else if (lowestBound) {
            result = {
                PRESSURE: pressure,
                OIL_FORMATION_VOLUME_FACTOR: lowestBound.OIL_FORMATION_VOLUME_FACTOR,
                GAS_FORMATION_VOLUME_FACTOR: lowestBound.GAS_FORMATION_VOLUME_FACTOR,
                WATER_FORMATION_VOLUME_FACTOR: lowestBound.WATER_FORMATION_VOLUME_FACTOR,
                SOLUTION_GAS_OIL_RATIO: lowestBound.SOLUTION_GAS_OIL_RATIO,
                VISCOSITY_OIL: lowestBound.VISCOSITY_OIL,
                VISCOSITY_WATER: lowestBound.VISCOSITY_WATER,
                VISCOSITY_GAS: lowestBound.VISCOSITY_GAS,
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: lowestBound.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: lowestBound.INJECTED_WATER_FORMATION_VOLUME_FACTOR
            };
        } else {
            result = {
                PRESSURE: pressure,
                OIL_FORMATION_VOLUME_FACTOR: null,
                GAS_FORMATION_VOLUME_FACTOR: null,
                WATER_FORMATION_VOLUME_FACTOR: null,
                SOLUTION_GAS_OIL_RATIO: null,
                VISCOSITY_OIL: null,
                VISCOSITY_WATER: null,
                VISCOSITY_GAS: null,
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: null,
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: null
            };
        }

        // Step 12: Round the results to 5 decimal places
        const roundedResult = {
            PRESSURE: Math.round(result.PRESSURE * 100000) / 100000,
            OIL_FORMATION_VOLUME_FACTOR: result.OIL_FORMATION_VOLUME_FACTOR ? Math.round(result.OIL_FORMATION_VOLUME_FACTOR * 100000) / 100000 : null,
            GAS_FORMATION_VOLUME_FACTOR: result.GAS_FORMATION_VOLUME_FACTOR ? Math.round(result.GAS_FORMATION_VOLUME_FACTOR * 100000) / 100000 : null,
            WATER_FORMATION_VOLUME_FACTOR: result.WATER_FORMATION_VOLUME_FACTOR ? Math.round(result.WATER_FORMATION_VOLUME_FACTOR * 100000) / 100000 : null,
            SOLUTION_GAS_OIL_RATIO: result.SOLUTION_GAS_OIL_RATIO ? Math.round(result.SOLUTION_GAS_OIL_RATIO * 100000) / 100000 : null,
            VISCOSITY_OIL: result.VISCOSITY_OIL ? Math.round(result.VISCOSITY_OIL * 100000) / 100000 : null,
            VISCOSITY_WATER: result.VISCOSITY_WATER ? Math.round(result.VISCOSITY_WATER * 100000) / 100000 : null,
            VISCOSITY_GAS: result.VISCOSITY_GAS ? Math.round(result.VISCOSITY_GAS * 100000) / 100000 : null,
            INJECTED_GAS_FORMATION_VOLUME_FACTOR: result.INJECTED_GAS_FORMATION_VOLUME_FACTOR ? Math.round(result.INJECTED_GAS_FORMATION_VOLUME_FACTOR * 100000) / 100000 : null,
            INJECTED_WATER_FORMATION_VOLUME_FACTOR: result.INJECTED_WATER_FORMATION_VOLUME_FACTOR ? Math.round(result.INJECTED_WATER_FORMATION_VOLUME_FACTOR * 100000) / 100000 : null
        };

        // Step 13: Write the result to the output table
        rowWriter.writeRow(roundedResult);
    }
}
$$;
