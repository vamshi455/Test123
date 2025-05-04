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
            SELECT LAST_DAY(TO_DATE(?, 'YYYY-MM-DD'), 'MONTH') AS last_day
        `;
        const lastDayStmt = context.prepare(lastDayQuery);
        lastDayStmt.execute({ binds: [vrr_date] });
        const lastDayResult = lastDayStmt.fetch();
        const lastDay = lastDayResult.getColumnValue("LAST_DAY");

        // Step 2: Fetch BasePVTData (equivalent to BasePVTData CTE)
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
            WHERE ID_COMPLETION = ?
              AND TEST_DATE <= ?
            ORDER BY TEST_DATE DESC
        `;
        const basePVTStmt = context.prepare(basePVTQuery);
        basePVTStmt.execute({ binds: [completion, lastDay] });
        const basePVTData = [];
        while (basePVTStmt.fetch()) {
            basePVTData.push({
                ID_COMPLETION: basePVTStmt.getColumnValue("ID_COMPLETION"),
                TEST_DATE: basePVTStmt.getColumnValue("TEST_DATE"),
                PRESSURE: basePVTStmt.getColumnValue("PRESSURE"),
                OIL_FORMATION_VOLUME_FACTOR: basePVTStmt.getColumnValue("OIL_FORMATION_VOLUME_FACTOR"),
                GAS_FORMATION_VOLUME_FACTOR: basePVTStmt.getColumnValue("GAS_FORMATION_VOLUME_FACTOR"),
                WATER_FORMATION_VOLUME_FACTOR: basePVTStmt.getColumnValue("WATER_FORMATION_VOLUME_FACTOR"),
                SOLUTION_GAS_OIL_RATIO: basePVTStmt.getColumnValue("SOLUTION_GAS_OIL_RATIO"),
                VISCOSITY_OIL: basePVTStmt.getColumnValue("VISCOSITY_OIL"),
                VISCOSITY_WATER: basePVTStmt.getColumnValue("VISCOSITY_WATER"),
                VISCOSITY_GAS: basePVTStmt.getColumnValue("VISCOSITY_GAS"),
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: basePVTStmt.getColumnValue("INJECTED_GAS_FORMATION_VOLUME_FACTOR"),
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: basePVTStmt.getColumnValue("INJECTED_WATER_FORMATION_VOLUME_FACTOR")
            });
        }

        // Step 3: Compute END_DATE for each record (equivalent to PVTwithEndDate CTE)
        const pvtWithEndDate = [];
        for (let i = 0; i < basePVTData.length; i++) {
            const record = basePVTData[i];
            // Compute NEXT_TEST_DATE (equivalent to LEAD)
            let nextTestDate = null;
            if (i < basePVTData.length - 1 && basePVTData[i + 1].ID_COMPLETION === record.ID_COMPLETION) {
                nextTestDate = basePVTData[i + 1].TEST_DATE;
            }
            const endDate = nextTestDate || '9999-12-31';

            // Apply the LAST_DAY(vrr_date, 'MONTH') < END_DATE filter
            if (new Date(lastDay) < new Date(endDate)) {
                pvtWithEndDate.push({
                    ...record,
                    END_DATE: endDate
                });
            }
        }

        // Step 4: Sort pvtWithEndDate by TEST_DATE DESC to ensure correct ordering for exact match
        pvtWithEndDate.sort((a, b) => new Date(b.TEST_DATE) - new Date(a.TEST_DATE));

        // Step 5: Find ExactMatch (most recent record where PRESSURE matches)
        let exactMatch = null;
        for (const record of pvtWithEndDate) {
            if (record.PRESSURE === pressure && record.ID_COMPLETION === completion && new Date(record.TEST_DATE) <= new Date(lastDay)) {
                exactMatch = record;
                break; // Take the most recent (first match after sorting by TEST_DATE DESC)
            }
        }

        // Step 6: Create a copy of pvtWithEndDate and sort by PRESSURE to find the closest bounds
        const pvtSortedByPressure = [...pvtWithEndDate].sort((a, b) => a.PRESSURE - b.PRESSURE);

        // Step 7: Find Lowerbound (closest record where PRESSURE < pressure)
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

        // Step 8: Find Upperbound (closest record where PRESSURE > pressure)
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

        // Step 9: Find SecondBound (closest record where PRESSURE < upperbound.PRESSURE)
        let secondBound = null;
        let secondBoundDiff = Infinity;
        if (upperbound) {
            for (const record of pvtSortedByPressure) {
                if (record.PRESSURE < upperbound.PRESSURE && record.ID_COMPLETION === completion && new Date(record.TEST_DATE) <= new Date(lastDay)) {
                    const diff = upperbound.PRESSURE - record.PRESSURE;
                    if (diff < secondBoundDiff) {
                        secondBound = record;
                        secondBoundDiff = diff;
                    }
                }
            }
        }

        // Step 10: Compute the interpolated or extrapolated values
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
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            `;
            const interpolateStmt = context.prepare(interpolateQuery);
            interpolateStmt.execute({ binds: [
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.OIL_FORMATION_VOLUME_FACTOR, upperbound.OIL_FORMATION_VOLUME_FACTOR,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.GAS_FORMATION_VOLUME_FACTOR, upperbound.GAS_FORMATION_VOLUME_FACTOR,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.WATER_FORMATION_VOLUME_FACTOR, upperbound.WATER_FORMATION_VOLUME_FACTOR,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.SOLUTION_GAS_OIL_RATIO, upperbound.SOLUTION_GAS_OIL_RATIO,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.VISCOSITY_OIL, upperbound.VISCOSITY_OIL,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.VISCOSITY_WATER, upperbound.VISCOSITY_WATER,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.VISCOSITY_GAS, upperbound.VISCOSITY_GAS,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.INJECTED_GAS_FORMATION_VOLUME_FACTOR, upperbound.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                pressure, lowerbound.PRESSURE, upperbound.PRESSURE, lowerbound.INJECTED_WATER_FORMATION_VOLUME_FACTOR, upperbound.INJECTED_WATER_FORMATION_VOLUME_FACTOR
            ]});
            const interpolateResult = interpolateStmt.fetch();
            result = {
                PRESSURE: pressure,
                OIL_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue("OIL_FORMATION_VOLUME_FACTOR"),
                GAS_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue("GAS_FORMATION_VOLUME_FACTOR"),
                WATER_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue("WATER_FORMATION_VOLUME_FACTOR"),
                SOLUTION_GAS_OIL_RATIO: interpolateResult.getColumnValue("SOLUTION_GAS_OIL_RATIO"),
                VISCOSITY_OIL: interpolateResult.getColumnValue("VISCOSITY_OIL"),
                VISCOSITY_WATER: interpolateResult.getColumnValue("VISCOSITY_WATER"),
                VISCOSITY_GAS: interpolateResult.getColumnValue("VISCOSITY_GAS"),
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue("INJECTED_GAS_FORMATION_VOLUME_FACTOR"),
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: interpolateResult.getColumnValue("INJECTED_WATER_FORMATION_VOLUME_FACTOR")
            };
        } else if (lowerbound && secondBound && upperbound) {
            const extrapolateQuery = `
                SELECT 
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            `;
            const extrapolateStmt = context.prepare(extrapolateQuery);
            extrapolateStmt.execute({ binds: [
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.OIL_FORMATION_VOLUME_FACTOR, secondBound.OIL_FORMATION_VOLUME_FACTOR,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.GAS_FORMATION_VOLUME_FACTOR, secondBound.GAS_FORMATION_VOLUME_FACTOR,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.WATER_FORMATION_VOLUME_FACTOR, secondBound.WATER_FORMATION_VOLUME_FACTOR,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.SOLUTION_GAS_OIL_RATIO, secondBound.SOLUTION_GAS_OIL_RATIO,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.VISCOSITY_OIL, secondBound.VISCOSITY_OIL,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.VISCOSITY_WATER, secondBound.VISCOSITY_WATER,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.VISCOSITY_GAS, secondBound.VISCOSITY_GAS,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.INJECTED_GAS_FORMATION_VOLUME_FACTOR, secondBound.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                pressure, upperbound.PRESSURE, secondBound.PRESSURE, upperbound.INJECTED_WATER_FORMATION_VOLUME_FACTOR, secondBound.INJECTED_WATER_FORMATION_VOLUME_FACTOR
            ]});
            const extrapolateResult = extrapolateStmt.fetch();
            result = {
                PRESSURE: pressure,
                OIL_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue("OIL_FORMATION_VOLUME_FACTOR"),
                GAS_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue("GAS_FORMATION_VOLUME_FACTOR"),
                WATER_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue("WATER_FORMATION_VOLUME_FACTOR"),
                SOLUTION_GAS_OIL_RATIO: extrapolateResult.getColumnValue("SOLUTION_GAS_OIL_RATIO"),
                VISCOSITY_OIL: extrapolateResult.getColumnValue("VISCOSITY_OIL"),
                VISCOSITY_WATER: extrapolateResult.getColumnValue("VISCOSITY_WATER"),
                VISCOSITY_GAS: extrapolateResult.getColumnValue("VISCOSITY_GAS"),
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue("INJECTED_GAS_FORMATION_VOLUME_FACTOR"),
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: extrapolateResult.getColumnValue("INJECTED_WATER_FORMATION_VOLUME_FACTOR")
            };
        } else if (upperbound) {
            result = {
                PRESSURE: upperbound.PRESSURE,
                OIL_FORMATION_VOLUME_FACTOR: upperbound.OIL_FORMATION_VOLUME_FACTOR,
                GAS_FORMATION_VOLUME_FACTOR: upperbound.GAS_FORMATION_VOLUME_FACTOR,
                WATER_FORMATION_VOLUME_FACTOR: upperbound.WATER_FORMATION_VOLUME_FACTOR,
                SOLUTION_GAS_OIL_RATIO: upperbound.SOLUTION_GAS_OIL_RATIO,
                VISCOSITY_OIL: upperbound.VISCOSITY_OIL,
                VISCOSITY_WATER: upperbound.VISCOSITY_WATER,
                VISCOSITY_GAS: upperbound.VISCOSITY_GAS,
                INJECTED_GAS_FORMATION_VOLUME_FACTOR: upperbound.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                INJECTED_WATER_FORMATION_VOLUME_FACTOR: upperbound.INJECTED_WATER_FORMATION_VOLUME_FACTOR
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

        // Step 11: Round the results to 5 decimal places
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

        // Step 12: Write the result to the output table
        rowWriter.writeRow(roundedResult);
    }
}
$$;
